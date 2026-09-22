// Does a SO_REUSEPORT listener that a BPF selection program never picks still
// accrue a backlog?
//
// Ringline's merged accept mode wants to take an overloaded worker out of the
// accept rotation without closing its listener (closing one RESETS whatever is
// already queued on it). That only works if "not selected" also means "not
// queued on". This measures it directly: N listeners in one reuseport group, a
// classic-BPF program that always returns index 0, then a burst of connects,
// then a non-blocking accept sweep counting what each socket actually holds.
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/filter.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef SO_ATTACH_REUSEPORT_CBPF
#define SO_ATTACH_REUSEPORT_CBPF 51
#endif

#define NLISTEN 4
#define NCONN 40
#define BACKLOG 128

static int make_listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); exit(1); }
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) { perror("SO_REUSEADDR"); exit(1); }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0) { perror("SO_REUSEPORT"); exit(1); }
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0) { perror("bind"); exit(1); }
    if (listen(fd, BACKLOG) < 0) { perror("listen"); exit(1); }
    return fd;
}

// Classic BPF whose return value is the index into the reuseport group.
static int attach_select(int fd, int index) {
    struct sock_filter code[] = {
        { BPF_RET | BPF_K, 0, 0, (unsigned int)index },
    };
    struct sock_fprog prog = { .len = 1, .filter = code };
    return setsockopt(fd, SOL_SOCKET, SO_ATTACH_REUSEPORT_CBPF, &prog, sizeof(prog));
}

static int drain(int fd) {
    int n = 0;
    for (;;) {
        int c = accept4(fd, NULL, NULL, SOCK_NONBLOCK);
        if (c < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            perror("accept4");
            break;
        }
        close(c);
        n++;
    }
    return n;
}

int main(int argc, char **argv) {
    int port = (argc > 1) ? atoi(argv[1]) : 24601;
    int select_index = (argc > 2) ? atoi(argv[2]) : 0;

    int fds[NLISTEN];
    for (int i = 0; i < NLISTEN; i++) fds[i] = make_listener(port);

    // The LISTENER must be non-blocking, or the accept sweep parks forever on
    // the first empty queue — which is exactly the socket we expect to be empty.
    for (int i = 0; i < NLISTEN; i++) {
        int fl = fcntl(fds[i], F_GETFL, 0);
        if (fl < 0 || fcntl(fds[i], F_SETFL, fl | O_NONBLOCK) < 0) {
            perror("F_SETFL O_NONBLOCK");
            return 1;
        }
    }

    // A negative index means: attach nothing, let the kernel's 4-tuple hash
    // spread them. That is the contrast case — it shows what the distribution
    // looks like with no steering at all.
    if (select_index < 0) {
        printf("no BPF attached: kernel 4-tuple hash over %d listeners on port %d\n",
               NLISTEN, port);
    } else {
        if (attach_select(fds[0], select_index) < 0) {
            fprintf(stderr, "SO_ATTACH_REUSEPORT_CBPF failed: %s\n", strerror(errno));
            return 2;
        }
        printf("attached CBPF selecting index %d over %d listeners on port %d\n",
               select_index, NLISTEN, port);
    }

    int clients[NCONN];
    int connected = 0;
    for (int i = 0; i < NCONN; i++) {
        int c = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in a;
        memset(&a, 0, sizeof(a));
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.sin_port = htons(port);
        if (connect(c, (struct sockaddr *)&a, sizeof(a)) < 0) {
            fprintf(stderr, "connect %d: %s\n", i, strerror(errno));
            close(c);
            continue;
        }
        clients[connected++] = c;
    }
    printf("connected %d of %d\n", connected, NCONN);

    printf("--- ss -lnt for port %d (Recv-Q is the pending backlog) ---\n", port);
    fflush(stdout);
    char cmd[128];
    snprintf(cmd, sizeof(cmd), "ss -lnt 'sport = :%d' || true", port);
    if (system(cmd) != 0) { /* informational only */ }
    fflush(stdout);

    int total = 0;
    for (int i = 0; i < NLISTEN; i++) {
        int n = drain(fds[i]);
        printf("listener[%d] accepted %d\n", i, n);
        total += n;
    }
    printf("total accepted %d of %d connected\n", total, connected);

    for (int i = 0; i < connected; i++) close(clients[i]);
    for (int i = 0; i < NLISTEN; i++) close(fds[i]);
    return 0;
}
