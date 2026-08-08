#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <cctype>
#include <vector>
#include <string>
#include <atomic>
#include <thread>
#include <chrono>
#include <memory>

constexpr int LISTEN_PORT = 8080;
constexpr int MAX_EVENTS = 64;
constexpr int HEALTH_CHECK_INTERVAL_S = 5;
constexpr int HEALTH_CHECK_TIMEOUT_MS = 1000;

struct Backend
{
	std::string ip;
	int port;
	std::atomic<bool> healthy{true};
	
	Backend(std::string ip_, int port_) : ip(std::move(ip_)), port(port_) {}
};
std::vector<std::unique_ptr<Backend>> all_backends;

struct Route
{
	std::string host;
	std::vector<Backend*> backends;
	size_t next_index = 0;
};

// M2: instead of one hardcoded backend, we now have a pool to rotate across.
// Start three dummy_backend instances on these ports to test round robin:
//   ./dummy_backend 9000
//   ./dummy_backend 9001
//   ./dummy_backend 9002
std::vector<std::unique_ptr<Route>> routes;
Route* default_route =  nullptr;

static void init_backends_and_routes()
{
	all_backends.push_back(std::make_unique<Backend>("127.0.0.1", 9000));
	all_backends.push_back(std::make_unique<Backend>("127.0.0.1", 9001));
	all_backends.push_back(std::make_unique<Backend>("127.0.0.1", 9002));
	
	Backend* b9000 = all_backends[0].get();
	Backend* b9001 = all_backends[1].get();
	Backend* b9002 = all_backends[2].get();
	
	auto api = std::make_unique<Route>();
	api->host = "api.local";
	api->backends = {b9000, b9001};
	routes.push_back(std::move(api));
	
	auto stat = std::make_unique<Route>();
	stat->host = "static.local";
	stat->backends = {b9002};
	routes.push_back(std::move(stat));
	
	auto def = std::make_unique<Route>();
	def->host = ""; // matches nothing by name - used as fallback
	def->backends = {b9000, b9001, b9002};
	routes.push_back(std::move(def));
	default_route = routes.back().get();
}

// Finds the route whose configured host matches the client's Host header
// (ignoring a ":port" suffix if present), falling back to the default
// route if there's no match or no Host header at all.
static Route* find_route(const std::string& host)
{
	std::string host_only = host;
	size_t colon = host_only.find(':');
	if(colon != std::string::npos) host_only = host_only.substr(0, colon);
	
	for(auto& r: routes)
	{
		if(!r->host.empty() && r->host == host_only) return r.get();
	}
	
	return default_route;
}

// Tracks which backend gets the *next* request. Advances every call.
// size_t next_backend_index = 0;

// Round robin: hand back the backend at the current index, then move the
// index forward, wrapping back to 0 once we reach the end of the list.
static Backend* get_next_backend(Route* route)
{
	auto& backends = route->backends;
	size_t start = route->next_index;
	for(size_t i = 0; i < backends.size(); i++)
	{
		size_t idx = (start + i) % backends.size();
		if(backends[idx]->healthy.load())
		{
			route->next_index = (idx + 1) % backends.size();
			return backends[idx];
		}
	}
	return nullptr; // every backend is down
}

// ---------------------------------------------------------------------------
// Per-connection state machine
//
// A single client request now flows through several *separate* epoll events
// instead of one blocking function call. This struct is how we remember
// "where we were" between one event and the next.
// ---------------------------------------------------------------------------
enum class ConnState
{
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
struct SocketCtx
{
	Connection* conn;
	bool is_backend;
};

struct Connection
{
	int client_fd  = -1;
	int backend_fd = -1;
	ConnState state = ConnState::READING_REQUEST;
	Backend* backend = nullptr;

	std::string request;              // bytes read from the client
	std::string response;             // bytes read from the backend
	size_t request_offset  = 0;       // how much of `request` we've sent so far
	size_t response_offset = 0;       // how much of `response` we've sent so far
	
	bool headers_parsed = false;
	size_t header_end = std::string::npos;
	size_t content_length = 0;
	std::string host;
	bool keep_alive = true;

	SocketCtx* client_ctx  = nullptr;
	SocketCtx* backend_ctx = nullptr;
};

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

static void reset_for_next_request(int epfd, Connection* conn)
{
	if(conn->backend_fd >= 0)
	{
		epoll_ctl(epfd, EPOLL_CTL_DEL, conn->backend_fd, nullptr);
		close(conn->backend_fd);
		conn->backend_fd = -1;
	}
	
	delete conn->backend_ctx;
	conn->backend_ctx = nullptr;
	conn->backend = nullptr;
	
	conn->request.clear();
	conn->response.clear();
	conn->request_offset = 0;
	conn->response_offset = 0;
	
	conn->headers_parsed = false;
	conn->header_end = std::string::npos;
	conn->content_length = 0;
	conn->host.clear();
	conn->keep_alive = true;
	
	conn->state = ConnState::READING_REQUEST;
	epoll_modify(epfd, conn->client_fd, EPOLLIN, conn->client_ctx);
}

static std::string to_lower(std::string s)
{
	for(auto& c : s) c = (char)std::tolower((unsigned char)c);
	return s;
}

static bool find_header_end(const std::string& buf, size_t& end_pos)
{
	size_t pos = buf.find("\r\n\r\n");
	if(pos == std::string::npos) return false;
	end_pos = pos + 4;
	return true;
}

static void parse_headers(Connection* conn)
{
	const std::string& buf = conn->request;
	
	size_t line_end = buf.find("\r\n");
	if(line_end == std::string::npos) return;
	std::string request_line = buf.substr(0, line_end);
	size_t sp1 = request_line.find(' ');
	size_t sp2 = (sp1 == std::string::npos) ? std::string::npos : request_line.find(' ', sp1 + 1);
	std::string version = (sp2 == std::string::npos) ? "" : request_line.substr(sp2 + 1);
	
	conn->keep_alive = (version == "HTTP/1.1");
	
	size_t pos = line_end + 2;
	while(pos < conn->header_end)
	{
		size_t next = buf.find("\r\n", pos);
		if(next == std::string::npos || next > conn->header_end) break;
		
		std::string line = buf.substr(pos, next - pos);
		if(line.empty()) break;
		
		size_t colon = line.find(':');
		if(colon != std::string::npos)
		{
			std::string key = to_lower(line.substr(0, colon));
			std::string value = line.substr(colon + 1);
			size_t vstart = value.find_first_not_of(' ');
			if(vstart != std::string::npos) value = value.substr(vstart);
			
			if(key == "host")
			{
				conn->host = value;
			}
			else if(key == "content-length")
			{
				conn->content_length = std::strtoul(value.c_str(), nullptr, 10);
			}
			else if(key == "connection")
			{
				std::string value_lower = to_lower(value);
				if(value_lower.find("close") != std::string::npos)
				{
					conn->keep_alive = false;
				}
			}
		}
		pos = next + 2;
	}
}

static std::string build_error_response(int code, const char* status_text, const char* body)
{
	char resp[512];
	snprintf(resp, sizeof(resp),
		"HTTP/1.1 %d %s\r\n"
		"Content-Type: text/plain\r\n"
		"Content-Length: %zu\r\n"
		"Connection: close\r\n"
		"\r\n"
		"%s",
		code, status_text, strlen(body), body);
	
	return std::string(resp);
}

static void begin_backend_connect(int epfd, Connection* conn, Route* route)
{
	Backend* backend = get_next_backend(route);
	if(!backend)
	{
		fprintf(stderr, "No healthy backends for route (host='%s')! Returning 503 to fd %d.\n", conn->host.c_str(), conn->client_fd);
		conn->response   = build_error_response(503, "Service Unavailable", "No healthy backend available for this route.\n");
		conn->keep_alive = false;
		conn->state = ConnState::SENDING_RESPONSE;
		epoll_modify(epfd, conn->client_fd, EPOLLOUT, conn->client_ctx);
		return;
	}
	
	printf("Connecting to backend %s:%d for fd %d (host='%s')...\n", backend->ip.c_str(), backend->port, conn->client_fd, conn->host.empty() ? "(none)" : conn->host.c_str());
	conn->backend = backend;
	
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
	backend_addr.sin_port = htons(backend->port);
	inet_pton(AF_INET, backend->ip.c_str(), &backend_addr.sin_addr);
	
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
		backend->healthy.store(false);
		fprintf(stderr, "Marking backend %s:%d UNHEALTHY (passive check)\n", backend->ip.c_str(), backend->port);
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
		if(!conn->headers_parsed)
		{
			size_t end_pos;
			if(!find_header_end(conn->request, end_pos))
			{
				return;
			}
			conn->header_end = end_pos;
			parse_headers(conn);
			conn->headers_parsed = true;
		}
		
		size_t total_needed = conn->header_end + conn->content_length;
		if(conn->request.size() < total_needed)
		{
			return;
		}
		
		printf("Full request from client fd %d (host= '%s', %zu header bytes, %zu body bytes)\n", conn->client_fd, conn->host.empty() ? "(none)" : conn->host.c_str(), conn->header_end, conn->content_length);		
		epoll_modify(epfd, conn->client_fd, 0, conn->client_ctx);
		Route* route = find_route(conn->host);
		begin_backend_connect(epfd, conn, route);
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
			if(conn->keep_alive)
			{
				printf("Finished response for fd %d, keeping connection alive.\n", conn->client_fd);
				reset_for_next_request(epfd, conn);
			}
			else
			{
				 printf("Finished response for fd %d, closing.\n", conn->client_fd);
				 close_connection(epfd, conn);
			}
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
			if (conn->backend)
			{
				conn->backend->healthy.store(false);
				fprintf(stderr, "Marking backend %s:%d UNHEALTHY (passive check)\n",
				conn->backend->ip.c_str(), conn->backend->port);
			}
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
			if(errno == EAGAIN || errno == EWOULDBLOCK) return;
			close_connection(epfd, conn);
			return;
		}
		if(n > 0)
		{
			conn->response.append(buf, n);
			return;
		}
		
		printf("Backend response complete (%zu bytes) for client fd %d, sending to client...\n", conn->response.size(), conn->client_fd);
		
		epoll_ctl(epfd, EPOLL_CTL_DEL, conn->backend_fd, nullptr);
		close(conn->backend_fd);
		conn->backend_fd = -1;
		delete conn->backend_ctx;
		conn->backend_ctx = nullptr;
		
		conn->state = ConnState::SENDING_RESPONSE;
		epoll_modify(epfd, conn->client_fd, EPOLLOUT, conn->client_ctx);
	}
}

static bool tcp_probe(const std::string& ip, int port, int timeout_ms)
{
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if(fd < 0) return false;
	set_nonblocking(fd);
	
	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
	
	int rc = connect(fd, (sockaddr*)&addr, sizeof(addr));
	if(rc == 0)
	{
		close(fd);
		return true;
	}
	
	if(errno != EINPROGRESS)
	{
		close(fd);
		return false;
	}
	pollfd pfd{};
	pfd.fd = fd;
	pfd.events = POLLOUT;
	
	int ready = poll(&pfd, 1, timeout_ms);
	if(ready <= 0)
	{
		close(fd);
		return false;
	}
	
	int err = 0;
	socklen_t len = sizeof(err);
	getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
	close(fd);
	return err == 0;
}

static void health_check_loop()
{
	while(true)
	{
		for (auto& backend_ptr : all_backends)
		{
			Backend& backend = *backend_ptr;
			bool ok = tcp_probe(backend.ip, backend.port, HEALTH_CHECK_TIMEOUT_MS);
			bool was_healthy = backend.healthy.load();
			
			if(ok != was_healthy)
			{
				backend.healthy.store(ok);
				printf("[health check] backend %s:%d is now %s\n", backend.ip.c_str(), backend.port, ok? "HEALTHY" : "UNHEALTHY");
			}
		}
		std::this_thread::sleep_for(std::chrono::seconds(HEALTH_CHECK_INTERVAL_S));
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
	init_backends_and_routes();
	
	std::thread health_thread(health_check_loop);
	health_thread.detach();
	
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
				
				Connection* conn = new Connection();
				conn->client_fd = client_fd;
				conn->client_ctx = new SocketCtx{conn, false};
				
				epoll_register(epfd, client_fd, EPOLLIN, conn->client_ctx);
			}
			else
			{
				
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
