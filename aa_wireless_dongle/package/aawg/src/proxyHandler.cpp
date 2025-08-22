#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <thread>
#include <optional>
#include <atomic>
#include <string>

#include "common.h"
#include "usb.h"
#include "bluetoothHandler.h"
#include "proxyHandler.h"

#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>

#define QUEUE_SIZE 256
#define MAX_MSG_LEN 16384

typedef struct {
    unsigned char buffer[QUEUE_SIZE][MAX_MSG_LEN];
    unsigned int msg_len[QUEUE_SIZE];
    std::atomic<size_t> head;
    std::atomic<size_t> tail;
} MessageQueue;
MessageQueue phone_to_hu_queue, hu_to_phone_queue;

// Initialize the queue
static inline void mq_init(MessageQueue *q) {
    atomic_init(&q->head, 0);
    atomic_init(&q->tail, 0);
}


void empty_signal_handler(int signal) {
    // Empty. We don't want to do anything but interrupt the thread.
}

ssize_t AAWProxy::readFully(int fd, unsigned char *buffer, size_t nbyte) {
    size_t remaining_bytes = nbyte;
    while (remaining_bytes > 0) {
        ssize_t len = read(fd, buffer, remaining_bytes);

        if (len <= 0) {
            // Error, cannot read more.
            return len;
        }

        buffer += len;
        remaining_bytes -= len;
    }

    return nbyte;
}

ssize_t AAWProxy::readMessage(int fd, unsigned char *buffer, size_t buffer_len) {
    size_t header_length = 4;
    if (ssize_t len = readFully(fd, buffer, header_length); len <= 0) {
        return len;
    }

    size_t message_length = (buffer[2] << 8) + buffer[3];

    constexpr char FRAME_TYPE_FIRST = 1 << 0;
    constexpr char FRAME_TYPE_LAST = 1 << 1;
    constexpr char FRAME_TYPE_MASK = FRAME_TYPE_FIRST | FRAME_TYPE_LAST;
    if ((buffer[1] & FRAME_TYPE_MASK) == FRAME_TYPE_FIRST) { // This means the header is 8 bytes long, we need to read four more bytes.
        message_length += 4;
    }

    if ((header_length + message_length) > buffer_len) {
        // Not enough space in the buffer. This is unexpected.
        errno = EMSGSIZE;
        return -1;
    }

    if (ssize_t len = readFully(fd, buffer + header_length, message_length); len <= 0) {
        return len;
    }

    return header_length + message_length;
}

void AAWProxy::stopForwarding(std::atomic<bool>& should_exit) {
    Logger::instance()->info("Interrupting threads to stop forwarding\n");
    should_exit = true;

    if (m_usb_tcp_thread) {
        pthread_kill(m_usb_tcp_thread->native_handle(), SIGUSR1);
    }

    if (m_tcp_usb_thread) {
        pthread_kill(m_tcp_usb_thread->native_handle(), SIGUSR1);
    }
}

ssize_t readFull(int fd, unsigned char *buffer, size_t nbyte) {
    size_t remaining_bytes = nbyte;
    while (remaining_bytes > 0) {
        ssize_t len = read(fd, buffer, remaining_bytes);

        if (len <= 0) {
            // Error, cannot read more.
            Logger::instance()->info("%s: Read returned error, %s\n", __FUNCTION__, strerror(errno));
            return len;
        }

        buffer += len;
        remaining_bytes -= len;
    }

    return nbyte;
}

ssize_t readTCP(int fd, unsigned char *buffer, size_t buffer_len) {
    size_t header_length = 4;
    if (ssize_t len = readFull(fd, buffer, header_length); len <= 0) {
        Logger::instance()->info("%s: Could not read any valid message\n", __FUNCTION__);
        return len;
    }

    size_t message_length = (buffer[2] << 8) + buffer[3];

    constexpr char FRAME_TYPE_FIRST = 1 << 0;
    constexpr char FRAME_TYPE_LAST = 1 << 1;
    constexpr char FRAME_TYPE_MASK = FRAME_TYPE_FIRST | FRAME_TYPE_LAST;
    if ((buffer[1] & FRAME_TYPE_MASK) == FRAME_TYPE_FIRST) { // This means the header is 8 bytes long, we need to read four more bytes.
        message_length += 4;
    }

    if ((header_length + message_length) > buffer_len) {
        // Not enough space in the buffer. This is unexpected.
        errno = EMSGSIZE;
        return -1;
    }

    if (ssize_t len = readFull(fd, buffer + header_length, message_length); len <= 0) {
        Logger::instance()->info("%s: Got a message header without body\n", __FUNCTION__);
        return len;
    }

    return header_length + message_length;
}

void enqueue_hu_message(int read_fd, std::atomic<bool>& should_exit) {
    size_t len = 0;
    while(!should_exit){
        size_t head = atomic_load_explicit(&hu_to_phone_queue.head, std::memory_order_relaxed);
        size_t next_head = (head + 1) % QUEUE_SIZE;
        size_t tail = atomic_load_explicit(&hu_to_phone_queue.tail, std::memory_order_acquire);

        if (next_head != tail){
            Logger::instance()->info("%s: waiting for next message from phone\n", __FUNCTION__);
            len = read(read_fd, hu_to_phone_queue.buffer[head], MAX_MSG_LEN);
            if(len > 0){
                Logger::instance()->info("%s: Queued message from phone, len:%d(%d-%d)\n", __FUNCTION__, len, tail, head);
                hu_to_phone_queue.msg_len[head] = len;
            } else {
                 next_head = head;
                 should_exit = true;
            }
        } else { // Queue full
            next_head = head;
        }
        atomic_store_explicit(&hu_to_phone_queue.head, next_head, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }            
}
// Producer: Add a message to the queue. Returns 1 on success, 0 if full.
void enqueue_phone_message(int read_fd, std::atomic<bool>& should_exit) {
    size_t len = 0;
    while(!should_exit){
        size_t head = atomic_load_explicit(&phone_to_hu_queue.head, std::memory_order_relaxed);
        size_t next_head = (head + 1) % QUEUE_SIZE;
        size_t tail = atomic_load_explicit(&phone_to_hu_queue.tail, std::memory_order_acquire);
        if (next_head != tail) {
            Logger::instance()->info("%s: waiting for next message from phone\n", __FUNCTION__);
            len = readTCP(read_fd, phone_to_hu_queue.buffer[head], MAX_MSG_LEN);
            if(len > 0){
                Logger::instance()->info("%s: Queued message from phone, len:%d(%d-%d)\n", __FUNCTION__, len, tail, head);
                phone_to_hu_queue.msg_len[head] = len;
            } else {
                 next_head = head;
                 should_exit = true;
            }
        } else {// Queue full
            next_head = head;
        }
        atomic_store_explicit(&phone_to_hu_queue.head, next_head, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void AAWProxy::forward(ProxyDirection direction, std::atomic<bool>& should_exit) {
    size_t buffer_len = 16384;
    unsigned char buffer[buffer_len];
    int write_fd;
    std::string read_name, write_name;
    MessageQueue *read_queue;
    int len = 0;
    size_t tail, head;

    switch (direction) {
        case ProxyDirection::TCP_to_USB:
            read_queue = &phone_to_hu_queue;
            write_fd = m_usb_fd;
            write_name = "USB";
            break;
        case ProxyDirection::USB_to_TCP:
            read_queue = &hu_to_phone_queue;
            write_fd = m_tcp_fd;
            write_name = "TCP";
            break;
    }

    while (!should_exit) {
        int tmp_tail, tmp_head;
        tail = atomic_load_explicit(&read_queue->tail, std::memory_order_relaxed);
        head = atomic_load_explicit(&read_queue->head, std::memory_order_acquire);
        tmp_tail = tail;
        tmp_head = head;

        if (tail != head){
            ssize_t wlen = write(write_fd, read_queue->buffer[tail], read_queue->msg_len[tail]);
            len = read_queue->msg_len[tail];
            atomic_store_explicit(&read_queue->tail, (tail + 1) % QUEUE_SIZE, std::memory_order_release);
            if (wlen <= 0) {
                // Start logging read/write details if there is an error.
                m_log_communication = true;
            }
            if (m_log_communication) {
                Logger::instance()->info("%d bytes written to %s(%d)[%d - %d]\n", wlen, write_name.c_str(), len, tmp_head, tmp_tail);
            }
            if (wlen < 0) {
                Logger::instance()->info("Write to %s failed: %s(%d)[%d - %d]\n", write_name.c_str(), strerror(errno), len, tmp_head, tmp_tail);
            }
        } else { // Queue empty
            atomic_store_explicit(&read_queue->tail, tail, std::memory_order_release);
        } 
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    stopForwarding(should_exit);
}

void AAWProxy::handleClient(int server_sock) {
    struct sockaddr client_address;
    socklen_t client_addresslen = sizeof(client_address);
    if ((m_tcp_fd = accept(server_sock, &client_address, &client_addresslen)) < 0) {
        close(server_sock);
        Logger::instance()->info("accept failed: %s\n", strerror(errno));
        return;
    }
    std::atomic<bool> should_exit = false;
    close(server_sock);
    // Set timeout on the TCP socket
    struct timeval tv = {
        .tv_sec = 10,
        .tv_usec = 0,
    };

    if (setsockopt(m_tcp_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv))) {
        Logger::instance()->info("setsockopt failed: %s\n", strerror(errno));
        return;
    }
    std::thread phone_to_queue(enqueue_phone_message, m_tcp_fd, std::ref(should_exit));

    Logger::instance()->info("Tcp server accepted connection\n");

    // Phone connected via TCP, we can stop retrying bluetooth connection
    BluetoothHandler::instance().stopConnectWithRetry();

    if (Config::instance()->getConnectionStrategy() != ConnectionStrategy::USB_FIRST) {
        if (!UsbManager::instance().enableDefaultAndWaitForAccessory(std::chrono::seconds(30))) {
            return;
        }
    }

    Logger::instance()->info("Opening usb accessory\n");
    if ((m_usb_fd = open("/dev/usb_accessory", O_RDWR)) < 0) {
        Logger::instance()->info("error opening /dev/usb_accessory: %s\n", strerror(errno));
        return;
    }
    std::thread usb_to_queue(enqueue_hu_message, m_usb_fd, std::ref(should_exit));
    // Setup signal handler
    struct sigaction sa;
    sa.sa_handler = empty_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGUSR1, &sa, NULL)) {
        Logger::instance()->info("Adding signal handler failed: %s\n", strerror(errno));
    }

    Logger::instance()->info("Forwarding data between TCP and USB\n");

    m_usb_tcp_thread = std::thread(&AAWProxy::forward, this, ProxyDirection::USB_to_TCP, std::ref(should_exit));
    m_tcp_usb_thread = std::thread(&AAWProxy::forward, this, ProxyDirection::TCP_to_USB, std::ref(should_exit));

    m_usb_tcp_thread->join();
    m_usb_tcp_thread = std::nullopt;

    m_tcp_usb_thread->join();
    m_tcp_usb_thread = std::nullopt;

    signal(SIGUSR1, SIG_DFL);

    close(m_usb_fd);
    m_usb_fd = -1;

    close(m_tcp_fd);
    m_tcp_fd = -1;

    Logger::instance()->info("Forwarding stopped\n");
}

std::optional<std::thread> AAWProxy::startServer(int32_t port) {
    mq_init(&phone_to_hu_queue);
    mq_init(&hu_to_phone_queue);

    Logger::instance()->info("Starting tcp server\n");
    int server_sock;
    if ((server_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        Logger::instance()->info("creating socket failed: %s\n", strerror(errno));
        return std::nullopt;
    }

    int opt = 1;
    if (setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        Logger::instance()->info("setsockopt failed: %s\n", strerror(errno));
        return std::nullopt;
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_sock, (struct sockaddr*)&address, sizeof(address)) < 0) {
        Logger::instance()->info("bind failed: %s\n", strerror(errno));
        return std::nullopt;
    }

    if (listen(server_sock, 3) < 0) {
        Logger::instance()->info("listen failed: %s\n", strerror(errno));
        return std::nullopt;
    }

    Logger::instance()->info("Tcp server listening on %d\n", port);

    return std::thread(&AAWProxy::handleClient, this, server_sock);
}
