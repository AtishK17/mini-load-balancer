// Same as dummy_backend, but deliberately sleeps before responding.
// Used only to prove the load balancer stays responsive to OTHER clients
// while this backend is being slow.
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? atoi(argv[1]) : 9000;
    int delay_seconds = (argc > 2) ? atoi(argv[2]) : 3;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    bind(fd, (sockaddr*)&addr, sizeof(addr));
    listen(fd, SOMAXCONN);

    printf("SLOW backend listening on port %d (delay=%ds)\n", port, delay_seconds);

    char body[64];
    snprintf(body, sizeof(body), "Slow reply from %d\n", port);
    char response[256];
    snprintf(response, sizeof(response),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n"
        "%s",
        strlen(body), body);

    while (true) {
        sockaddr_in client_addr{};
        socklen_t   client_len = sizeof(client_addr);
        int client_fd = accept(fd, (sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) continue;

        char buf[4096];
        read(client_fd, buf, sizeof(buf));

        sleep(delay_seconds);   // simulate a slow backend

        write(client_fd, response, strlen(response));
        close(client_fd);
    }

    close(fd);
    return 0;
}
