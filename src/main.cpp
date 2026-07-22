#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <string>

constexpr int LISTEN_PORT = 8080;
constexpr int MAX_EVENTS = 64;

struct Backend {
    std::string ip;
    int         port;
};

// M2: instead of one hardcoded backend, we now have a pool to rotate across.
// Start three dummy_backend instances on these ports to test round robin:
//   ./dummy_backend 9000
//   ./dummy_backend 9001
//   ./dummy_backend 9002
std::vector<Backend> backends = {
    {"127.0.0.1", 9000},
    {"127.0.0.1", 9001},
    {"127.0.0.1", 9002},
};

// Tracks which backend gets the *next* request. Advances every call.
size_t next_backend_index = 0;

// Round robin: hand back the backend at the current index, then move the
// index forward, wrapping back to 0 once we reach the end of the list.
static Backend& get_next_backend() {
    Backend& b = backends[next_backend_index];
    next_backend_index = (next_backend_index + 1) % backends.size();
    return b;
}


static void proxy_to_backend(int client_fd, const char* request, ssize_t request_len, const Backend& backend)
{
	int backend_fd = socket(AF_INET, SOCK_STREAM, 0);
	if(backend_fd < 0)
	{
		perror("socket");
		return;
	}
	
	sockaddr_in backend_addr{};
	backend_addr.sin_family = AF_INET;
	backend_addr.sin_port = htons(backend.port);
	inet_pton(AF_INET, backend.ip.c_str(), &backend_addr.sin_addr);
	
	if(connect(backend_fd, (sockaddr*)&backend_addr, sizeof(backend_addr)) < 0)
	{
		perror("connect to backend failed");
		close(backend_fd);
		return;
	}
	
	write(backend_fd, request, request_len);
	
	char response[8192];
	ssize_t n;
	while((n = read(backend_fd, response, sizeof(response))) > 0)
	{
		write(client_fd, response, n);
	}
	
	close(backend_fd);
}

// Make a file descriptor non-blocking. Required for epoll's edge-triggered
// mode: if a socket were blocking, a read() with no data ready would freeze
// the entire event loop, stalling every other connection.

static void set_nonblocking(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Create, bind, and listen on the port clients will connect to.
static int create_listen_socket(int port)
{
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if(fd < 0)
	{
		perror("socket");
		exit(1);
	}
	
	int opt = 1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	
	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY; // listen on all interfaces
	addr.sin_port = htons(port);
	
	if(bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0)
	{
		perror("bind");
		exit(1);
	}
	if(listen(fd, SOMAXCONN) < 0)
	{
		perror("listen");
		exit(1);
	}
	
	set_nonblocking(fd);
	return fd;
}

int main()
{
	int listen_fd = create_listen_socket(LISTEN_PORT);
	printf("Listening on port %d...\n", LISTEN_PORT);
	
	int epfd = epoll_create1(0);
	if(epfd < 0)
	{
		perror("epoll_create1");
		exit(1);
	}
	
	epoll_event ev{};
	ev.events = EPOLLIN;
	ev.data.fd = listen_fd;
	epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev);
	
	epoll_event events[MAX_EVENTS];
	
	printf("entering event loop.\n");
	while(true)
	{
		int n = epoll_wait(epfd, events, MAX_EVENTS, -1); // -1 = block forever;
		if(n < 0)
		{
			perror("epoll_wait");
			break;
		}
		
		for(int i = 0; i < n; i++)
		{
			if(events[i].data.fd == listen_fd)
			{
				// New incoming connection — accept it.
				sockaddr_in client_addr{};
				socklen_t client_len = sizeof(client_addr);
				int client_fd = accept(listen_fd, (sockaddr*)&client_addr, &client_len);
				if(client_fd < 0)
				{
					perror("accept");
					continue;
				}
				
				char ip_str[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
				printf("Accepted connection from %s:%d (fd=%d)\n", ip_str, ntohs(client_addr.sin_port), client_fd);
				
				set_nonblocking(client_fd);
				
				epoll_event client_ev{};
				client_ev.events = EPOLLIN;
				client_ev.data.fd = client_fd;
				epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &client_ev);
			}
			else
			{
				int client_fd = events[i].data.fd;
				char buf[8192];
				ssize_t n = read(client_fd, buf, sizeof(buf));
				
				if(n <= 0)
				{
					// n == 0: client closed the connection.
					// n < 0: read error.
					// Either way, clean up: epoll auto-removes closed fds,
					// but we still must close() ourselves to release it.
					epoll_ctl(epfd, EPOLL_CTL_DEL, client_fd, nullptr);
					close(client_fd);
					continue;
				}
				
				Backend& backend = get_next_backend();
				printf("Read %zd bytes from fd %d, forwarding to %s:%d...\n", n, client_fd, backend.ip.c_str(), backend.port);
				proxy_to_backend(client_fd, buf, n, backend);
				// Naive M1 behavior: one request per connection, then close.
				// Real HTTP keep-alive support comes later once we're
				// parsing headers properly (M4).
				epoll_ctl(epfd, EPOLL_CTL_DEL, client_fd, nullptr);
				close(client_fd);
			}
		}
	}
	
	close(listen_fd);
	close(epfd);
	return 0;
}
