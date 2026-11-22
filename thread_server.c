#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

static pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t all_done = PTHREAD_COND_INITIALIZER;
static int active_clients = 0;
static unsigned long total_clients = 0;
static int shutting_down = 0;
static volatile sig_atomic_t stop_signal = 0;

struct client_args {
    int fd;
    unsigned long number;
};

static void on_signal(int signo) {
    (void)signo;
    stop_signal = 1;
}

static int must_stop(void) {
    pthread_mutex_lock(&stats_mutex);
    int result = shutting_down;
    pthread_mutex_unlock(&stats_mutex);
    return result;
}

static int send_all(int fd, const char *buf, size_t size) {
    size_t sent = 0;
    while (sent < size) {
        if (must_stop()) return -1;
        ssize_t n = send(fd, buf + sent, size - sent, 0);
        if (n > 0) {
            sent += (size_t)n;
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            continue;
        } else {
            return -1;
        }
    }
    return 0;
}

static void *client_worker(void *ptr) {
    struct client_args *client = ptr;
    const int fd = client->fd;
    const unsigned long number = client->number;
    free(client);

    /* Таймаут нужен для корректной остановки, даже если клиент молчит. */
    struct timeval timeout = { .tv_sec = 0, .tv_usec = 300000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout);

    char buffer[4096];
    while (!must_stop()) {
        ssize_t n = recv(fd, buffer, sizeof buffer, 0);
        if (n > 0) {
            if (send_all(fd, buffer, (size_t)n) != 0) break;
        } else if (n == 0) {
            break;
        } else if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
        } else {
            break;
        }
    }

    close(fd);
    pthread_mutex_lock(&stats_mutex);
    --active_clients;
    int remaining = active_clients;
    if (active_clients == 0) pthread_cond_signal(&all_done);
    pthread_mutex_unlock(&stats_mutex);
    printf("Клиент %lu отключён. Активных: %d\n", number, remaining);
    fflush(stdout);
    return NULL;
}

static int make_listener(unsigned short port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int reuse = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse) < 0) {
        close(fd);
        return -1;
    }
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        close(fd);
        return -1;
    }
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) == -1 || listen(fd, 32) == -1) {
        close(fd);
        return -1;
    }
    return fd;
}

int main(int argc, char **argv) {
    unsigned short port = 9090;
    if (argc > 2) {
        fprintf(stderr, "Использование: %s [порт]\n", argv[0]);
        return 1;
    }
    if (argc == 2) {
        char *end = NULL;
        errno = 0;
        long number = strtol(argv[1], &end, 10);
        if (errno != 0 || *end != '\0' || number < 1 || number > 65535) {
            fprintf(stderr, "Порт должен быть от 1 до 65535\n");
            return 1;
        }
        port = (unsigned short)number;
    }

    struct sigaction sa = {0};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) < 0 || sigaction(SIGTERM, &sa, NULL) < 0) {
        perror("sigaction");
        return 1;
    }
    struct sigaction ignore_pipe = {0};
    ignore_pipe.sa_handler = SIG_IGN;
    sigemptyset(&ignore_pipe.sa_mask);
    if (sigaction(SIGPIPE, &ignore_pipe, NULL) < 0) {
        perror("sigaction(SIGPIPE)");
        return 1;
    }

    int listener = make_listener(port);
    if (listener == -1) {
        perror("socket/bind/listen");
        return 1;
    }
    printf("Сервер слушает 127.0.0.1:%u. Остановка: Ctrl+C\n", port);
    fflush(stdout);

    while (!stop_signal) {
        struct pollfd pfd = { .fd = listener, .events = POLLIN };
        int ready = poll(&pfd, 1, 200);
        if (ready == 0) continue;
        if (ready < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }
        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) break;
        if (!(pfd.revents & POLLIN)) continue;

        while (!stop_signal) {
            int fd = accept(listener, NULL, NULL);
            if (fd < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                perror("accept");
                break;
            }
            struct client_args *client = malloc(sizeof *client);
            if (!client) {
                close(fd);
                continue;
            }
            pthread_mutex_lock(&stats_mutex);
            client->fd = fd;
            client->number = ++total_clients;
            ++active_clients;
            int active_snapshot = active_clients;
            pthread_mutex_unlock(&stats_mutex);

            unsigned long client_number = client->number;
            pthread_t thread;
            int result = pthread_create(&thread, NULL, client_worker, client);
            if (result != 0) {
                fprintf(stderr, "pthread_create: %s\n", strerror(result));
                close(fd);
                free(client);
                pthread_mutex_lock(&stats_mutex);
                --active_clients;
                if (active_clients == 0) pthread_cond_signal(&all_done);
                pthread_mutex_unlock(&stats_mutex);
                continue;
            }
            pthread_detach(thread);
            printf("Клиент %lu подключён. Активных: %d\n", client_number, active_snapshot);
            fflush(stdout);
        }
    }

    close(listener);
    pthread_mutex_lock(&stats_mutex);
    shutting_down = 1;
    while (active_clients > 0) pthread_cond_wait(&all_done, &stats_mutex);
    unsigned long total = total_clients;
    pthread_mutex_unlock(&stats_mutex);
    printf("Сервер остановлен. Всего подключений: %lu, активных: 0\n", total);
    pthread_cond_destroy(&all_done);
    pthread_mutex_destroy(&stats_mutex);
    return 0;
}
