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
#include <cerrno>
#include <vector>
#include <string>

constexpr int LISTEN_PORT = 8080;
constexpr int MAX_EVENTS = 64;

struct Backend {
	std::string ip;
	int port;
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

// ---------------------------------------------------------------------------
// Per-connection state machine
//
// A single client request now flows through several *separate* epoll events
// instead of one blocking function call. This struct is how we remember
// "where we were" between one event and the next.
// ---------------------------------------------------------------------------
enum class ConnState {
	READING_REQUEST,     // waiting for the client to send its request
	CONNECTING_BACKEND,  // non-blocking connect() to backend in progress
	SENDING_REQUEST,     // writing the request to the backend (may take >1 write)
	READING_RESPONSE,    // accumulating the backend's response
	SENDING_RESPONSE,    // writing the response back to the client (may take >1 write)
};

struct Connection;

// Attached to epoll_data.ptr for every fd we register. Tells us which
// Connection this event belongs to, and whether it's the client or backend
// side of it — since one Connection owns two fds.
struct SocketCtx {
	Connection* conn;
	bool is_backend;
};

struct Connection {
	int client_fd  = -1;
	int backend_fd = -1;
	ConnState state = ConnState::READING_REQUEST;

	std::string request;              // bytes read from the client
	std::string response;             // bytes read from the backend
	size_t request_offset  = 0;       // how much of `request` we've sent so far
	size_t response_offset = 0;       // how much of `response` we've sent so far

	SocketCtx* client_ctx  = nullptr;
	SocketCtx* backend_ctx = nullptr;
};

/* static void proxy_to_backend(int client_fd, const char* request, ssize_t request_len, const Backend& backend)
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
} */

// Make a file descriptor non-blocking. Required for epoll's edge-triggered
// mode: if a socket were blocking, a read() with no data ready would freeze
// the entire event loop, stalling every other connection.

static void set_nonblocking(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void epoll_register(int epfd, int fd, uint32_t events, SocketCtx* ctx)
{
	epoll_event ev{};
	ev.events = events;
	ev.data.ptr = ctx;
	epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
}

static void epoll_modify(int epfd, int fd, uint32_t events, SocketCtx* ctx)
{
	epoll_event ev{};
	ev.events = events;
	ev.data.ptr = ctx;
	epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

static void close_connection(int epfd, Connection* conn)
{
	if(conn->client_fd >=0)
	{
		epoll_ctl(epfd, EPOLL_CTL_DEL, conn->client_fd, nullptr);
		close(conn->client_fd);
	}
	
	if(conn->backend_fd >= 0)
	{
		epoll_ctl(epfd, EPOLL_CTL_DEL, conn->backend_fd, nullptr);
		close(conn->backend_fd);
	}
	delete conn->client_ctx;
	delete conn->backend_ctx;
	delete conn;
}

static void begin_backend_connect(int epfd, Connection* conn)
{
	Backend& backend = get_next_backend();
	printf("Connection to backend %s:%d for fd %d...\n", backend.ip.c_str(), backend.port, conn->client_fd);
	
	int backend_fd = socket(AF_INET, SOCK_STREAM, 0);
	if(backend_fd < 0)
	{
		perror("socket");
		close_connection(epfd, conn);
		return;
	}
	set_nonblocking(backend_fd);
	
	sockaddr_in backend_addr{};
	backend_addr.sin_family = AF_INET;
	backend_addr.sin_port = htons(backend.port);
	inet_pton(AF_INET, backend.ip.c_str(), &backend_addr.sin_addr);
	
	conn->backend_fd = backend_fd;
	conn->backend_ctx = new SocketCtx{conn, true};
	
	int rc = connect(backend_fd, (sockaddr*)&backend_addr, sizeof(backend_addr));
	if(rc == 0)
	{
		// Connected immediately (uncommon, but happens for local backends).
		// We can go straight to sending the request.
		conn->state = ConnState::SENDING_REQUEST;
		epoll_register(epfd, backend_fd, EPOLLOUT, conn->backend_ctx);
	}
	else if(errno == EINPROGRESS)
	{
		// The normal case: the handshake is happening in the background.
		// Ask epoll to tell us the moment this fd becomes writable — that's
		// exactly the signal that the connection attempt has finished
		// (successfully or not).
		conn->state = ConnState::CONNECTING_BACKEND;
		epoll_register(epfd, backend_fd, EPOLLOUT, conn->backend_ctx);
	}
	else
	{
		perror("connect to backend failed");
		close_connection(epfd, conn);
	}
}

static void handle_client_event(int epfd, Connection* conn)
{
	if(conn->state == ConnState::READING_REQUEST)
	{
		char buf[8192];
		ssize_t n = read(conn->client_fd, buf, sizeof(buf));
		
		if(n <= 0)
		{
			if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
			close_connection(epfd, conn);
			return;
		}
		
		conn->request.append(buf, n);
		printf("Read %zd bytes from client fd %d, connection to backend...\n", n, conn->client_fd);
		
		epoll_modify(epfd, conn->client_fd, 0, conn->client_ctx);
		
		begin_backend_connect(epfd, conn);
	}
	else if(conn->state == ConnState::SENDING_RESPONSE)
	{
		const char* data = conn->response.data() + conn->response_offset;
		size_t len = conn->response.size() - conn->response_offset;
		
		ssize_t n = write(conn->client_fd, data, len);
		if(n < 0)
		{
			if(errno == EAGAIN || errno == EWOULDBLOCK) return;
			close_connection(epfd, conn);
			return;
		}
		
		conn->response_offset += n;
		if(conn->response_offset >= conn->response.size())
		{
			printf("Finish sending response to client fd %d, closing.\n", conn->client_fd);
			close_connection(epfd, conn);
		}
	}
}

static void handle_backend_event(int epfd, Connection* conn)
{
	if(conn->state == ConnState::CONNECTING_BACKEND)
	{
		int err = 0;
		socklen_t len = sizeof(err);
		getsockopt(conn->backend_fd, SOL_SOCKET, SO_ERROR, &err, &len);
		
		if(err != 0)
		{
			fprintf(stderr, "backend connect failed: %s\n", strerror(err));
			close_connection(epfd, conn);
			return;
		}
		
		printf("Backend connected for client fd %d, sending request...\n", conn->client_fd);
		conn->state = ConnState::SENDING_REQUEST;
	}
	
	if(conn->state == ConnState::SENDING_REQUEST)
	{
		const char* data = conn->request.data() + conn->request_offset;
		size_t len = conn->request.size() - conn->request_offset;
		
		ssize_t n = write(conn->backend_fd, data, len);
		if(n < 0)
		{
			if(errno == EAGAIN || errno == EWOULDBLOCK) return;
			close_connection(epfd, conn);
			return;
		}
		
		conn->request_offset += n;
		if(conn->request_offset >= conn->request.size())
		{
			conn->state = ConnState::READING_RESPONSE;
			epoll_modify(epfd, conn->backend_fd, EPOLLIN, conn->backend_ctx);
		}
		return;
	}
	if(conn->state == ConnState::READING_RESPONSE)
	{
		char buf[8192];
		ssize_t n = read(conn->backend_fd, buf, sizeof(buf));
		
		if(n < 0)
		{
			if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
			close_connection(epfd, conn);
			return;
		}
		if(n > 0)
		{
			conn->response.append(buf, n);
			return;
		}
		
		printf("Backend response complete (%zu bytes) for client fd %d, sending to client...\n", conn->response.size(), conn->client_fd);
		conn->state = ConnState::SENDING_RESPONSE;
		epoll_modify(epfd, conn->client_fd, EPOLLOUT, conn->client_ctx);
	}
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
	
	epoll_event listen_ev{};
	listen_ev.events = EPOLLIN;
	listen_ev.data.fd = listen_fd;
	epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &listen_ev);
	
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
				
				/* epoll_event client_ev{};
				client_ev.events = EPOLLIN;
				client_ev.data.fd = client_fd;
				epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &client_ev);
				*/
				Connection* conn = new Connection();
				conn->client_fd = client_fd;
				conn->client_ctx = new SocketCtx{conn, false};
				
				epoll_register(epfd, client_fd, EPOLLIN, conn->client_ctx);
			}
			else
			{
				/*
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
				*/
				
				SocketCtx* ctx = static_cast<SocketCtx*>(events[i].data.ptr);
				Connection* conn = ctx->conn;
				
				if(ctx->is_backend)
				{
					handle_backend_event(epfd, conn);
				}
				else
				{
					handle_client_event(epfd, conn);
				}
			}
		}
	}
	
	close(listen_fd);
	close(epfd);
	return 0;
}
