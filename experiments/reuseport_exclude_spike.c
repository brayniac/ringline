// Tier 2 primitive: can a CBPF reuseport program spread over a *subset* of the
// group, skipping excluded sockets?
//
// #451 proved a constant-index program works. That is enough to pin every
// connection to one socket, not to take one socket out while the rest keep
// sharing. This tries the shape ringline would actually attach:
//
//   idx = <ancillary CPU> % live_count
//   then a jump chain mapping idx -> the live socket's real group index
//
// Exclusion is all CBPF has to do here; balance is tier 1's fd handoff, so an
// uneven spread across the live set is fine. What must hold is that an
// excluded socket receives nothing.
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
#ifndef SKF_AD_OFF
#define SKF_AD_OFF (-0x1000)
#endif
#ifndef SKF_AD_CPU
#define SKF_AD_CPU 36
#endif
#ifndef SKF_AD_RANDOM
#define SKF_AD_RANDOM 56
#endif

#define NLISTEN 4
#define NCONN 40
#define BACKLOG 128
#define MAXPROG 64

static int make_listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); exit(1); }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0) { perror("SO_REUSEPORT"); exit(1); }
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0) { perror("bind"); exit(1); }
    if (listen(fd, BACKLOG) < 0) { perror("listen"); exit(1); }
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
    return fd;
}

// idx = <source> % live_count; then map idx -> live[idx] via a jump chain.
// `src_ad` is the ancillary load to index by: SKF_AD_CPU correlates with the
// receiving CPU (constant for a loopback client, so everything funnels to one
// socket), SKF_AD_RANDOM varies per SYN.
static int attach_live_set(int fd, const int *live, int live_count, int src_ad) {
    struct sock_filter code[MAXPROG];
    int n = 0;
    // A = cpu
    code[n++] = (struct sock_filter){ BPF_LD | BPF_W | BPF_ABS, 0, 0, (unsigned)(SKF_AD_OFF + src_ad) };
    // A = A % live_count
    code[n++] = (struct sock_filter){ BPF_ALU | BPF_MOD | BPF_K, 0, 0, (unsigned)live_count };
    // for each slot: if A == i -> return live[i]
    for (int i = 0; i < live_count; i++) {
        code[n++] = (struct sock_filter){ BPF_JMP | BPF_JEQ | BPF_K, 0, 1, (unsigned)i };
        code[n++] = (struct sock_filter){ BPF_RET | BPF_K, 0, 0, (unsigned)live[i] };
    }
    // fallback: first live socket
    code[n++] = (struct sock_filter){ BPF_RET | BPF_K, 0, 0, (unsigned)live[0] };

    struct sock_fprog prog = { .len = (unsigned short)n, .filter = code };
    return setsockopt(fd, SOL_SOCKET, SO_ATTACH_REUSEPORT_CBPF, &prog, sizeof(prog));
}

static int drain(int fd) {
    int n = 0;
    for (;;) {
        int c = accept(fd, NULL, NULL);
        if (c < 0) break;
        close(c);
        n++;
    }
    return n;
}

int main(int argc, char **argv) {
    int port = (argc > 1) ? atoi(argv[1]) : 24701;
    int excluded = (argc > 2) ? atoi(argv[2]) : 3;
    int src_ad = (argc > 3 && atoi(argv[3]) == 1) ? SKF_AD_RANDOM : SKF_AD_CPU;

    int fds[NLISTEN];
    for (int i = 0; i < NLISTEN; i++) fds[i] = make_listener(port);

    int live[NLISTEN], live_count = 0;
    for (int i = 0; i < NLISTEN; i++) if (i != excluded) live[live_count++] = i;

    if (attach_live_set(fds[0], live, live_count, src_ad) < 0) {
        fprintf(stderr, "attach failed: %s\n", strerror(errno));
        return 2;
    }
    printf("excluded socket %d; live set of %d; index source %s\n", excluded, live_count,
           src_ad == SKF_AD_RANDOM ? "RANDOM" : "CPU");

    int clients[NCONN], connected = 0;
    for (int i = 0; i < NCONN; i++) {
        int c = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in a;
        memset(&a, 0, sizeof(a));
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.sin_port = htons(port);
        if (connect(c, (struct sockaddr *)&a, sizeof(a)) < 0) { close(c); continue; }
        clients[connected++] = c;
    }
    printf("connected %d\n", connected);

    int total = 0;
    for (int i = 0; i < NLISTEN; i++) {
        int got = drain(fds[i]);
        printf("listener[%d]%s accepted %d\n", i, i == excluded ? " (EXCLUDED)" : "", got);
        total += got;
    }
    printf("total %d of %d\n", total, connected);

    for (int i = 0; i < connected; i++) close(clients[i]);
    for (int i = 0; i < NLISTEN; i++) close(fds[i]);
    return 0;
}
