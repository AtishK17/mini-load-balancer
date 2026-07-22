# mini-load-balancer

An L7 HTTP reverse-proxy load balancer, built from raw C++ sockets + epoll
(no frameworks), as a systems-design learning project and interview prep piece.

## Why raw sockets instead of a framework?

The goal is to actually understand what a load balancer does under the hood —
non-blocking I/O, the event loop, connection lifecycle, backend health and
routing — rather than have a framework hide it.

## Milestones

- [x] **M1** — Single-threaded epoll event loop, accept loop, naive proxy to
      one hardcoded backend (blocking connect/read to backend for now).
- [x] **M2** — Backend pool + round robin, then weighted round robin / least
      connections.
- [ ] **M3** — Active + passive health checks.
- [ ] **M4** — Minimal HTTP parsing, host-based routing, keep-alive support.
- [ ] **M5** — Stats endpoint + live dashboard.
- [ ] Stretch: config hot-reload, TLS termination, graceful shutdown.

## Build

```bash
mkdir build && cd build
cmake ..
make
```

## Run

Terminal 1 — start the dummy backend:
```bash
./build/dummy_backend 9000
```

Terminal 2 — start the load balancer:
```bash
./build/lb
```

Terminal 3 — send a request through it:
```bash
curl http://127.0.0.1:8080/
```
