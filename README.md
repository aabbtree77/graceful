# Graceful HTTP servers: Go vs Nim vs D

> A three-part comparison — shutdown semantics, concurrency primitives, and ergonomics.

## Contents

- [Part 1 — Graceful shutdown](#part-1--graceful-shutdown)
- [Part 2 — Primitives & verdict](#part-2--primitives--verdict)
- [Part 3 — Concurrency & parallelism](#part-3--concurrency--parallelism)

---

# Part 1 — Graceful shutdown

In go, a server with graceful shutdown is typically done like this: 

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"time"

	_ "github.com/mattn/go-sqlite3"

	dbpkg "schatzhauser/db"
	"schatzhauser/internal/config"
	"schatzhauser/internal/logger"
	"schatzhauser/internal/server"
)

func run(ctx context.Context, w io.Writer, args []string) error {

	// Load configuration
	cfg, err := config.LoadConfig("config.toml")
	if err != nil {
		logger.Error("failed to load config", "err", err)
	}

	// Setup logging: passing in *cfg to read cfg.Debug from toml
	// and set stdout to text (debug=true) or json (debug=false)
	logger.Init(cfg)
	logger.Info("starting schatzhauser", "debug", cfg.Debug)

	// -----------------------------------------------------
	// Database (SQLite)
	// -----------------------------------------------------
	db, err := sql.Open("sqlite3", cfg.DBPath+"?_foreign_keys=on")
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	// DB sanity check
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}

	// -----------------------------------------------------
	// Run migrations (manual, SQL files)
	// -----------------------------------------------------
	if err := dbpkg.RunMigrations(db); err != nil {

		return fmt.Errorf("migrations: %w", err)
	}

	// -----------------------------------------------------
	// HTTP server setup
	// -----------------------------------------------------
	mux := http.NewServeMux()

	server.RegisterRoutes(mux, db, &cfg)

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	// -----------------------------------------------------
	// Server in goroutine + graceful shutdown
	// -----------------------------------------------------
	go func() {
		logger.Info("listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
		}
	}()

	// Handle Ctrl+C
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "err", err)
	}

	_ = db.Close()
	return nil
}

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Stdout, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
} 
```

How is this done in Nim and D?


**deepseek:**

# Graceful HTTP shutdown in Nim and D

Both languages can do this, but the idioms differ because neither has a stdlib HTTP server with a Go-style `Shutdown(ctx)` that tracks in-flight requests. Here's how you'd mirror your `run()` shape.

## Nim (`std/asynchttpserver`)

Nim's stdlib server doesn't track active connections, so "graceful" mostly means "stop accepting, let the event loop finish cleanly." There's no built-in per-connection drain timeout — you have to track in-flight handlers yourself if you want that.

```nim
import asynchttpserver, asyncdispatch, std/[os, posix]

let server = newAsyncHttpServer()

proc handler(req: Request) {.async.} =
  # ~ srv handler
  await req.respond(Http200, "Hello World")

proc onSignal(sig: cint) {.noconv.} =
  # Set running=false and close the listening socket
  server.close()

proc run(): Future[void] {.async.} =
  echo "listening on :8080"
  await server.serve(Port(8080), handler)

when isMainModule:
  signal(SIGINT,  onSignal)   # from std/posix
  signal(SIGTERM, onSignal)
  waitFor run()
  echo "stopped"
```

Notes:
- `server.close()` flips `AsyncHTTPServer.running` to `false` and closes the listening socket; `serve()` returns on the next loop iteration. Existing request futures keep running on the dispatcher until `waitFor run()` returns.
- If you need a real drain window (like your 5 s timeout), keep a counter or `Channel` of in-flight requests and after `serve()` returns, `poll()` in a loop until it hits zero or a deadline passes.
- For a more production-grade server use **`httpbeast`** (multi-threaded, no graceful shutdown either) or **`chronos`** (`chronos/apps/http/httpclient` + `HttpServer`), which gives you `server.stop()` / `server.closeWait()` and more control.
- If you use **Jester**, same pattern: hold the server handle, `server.close()` from the signal hook.

## D (`vibe.d` / `vibe-http`)

Vibe.d *does* have graceful shutdown semantics: `HTTPListener.stopListening()` stops accepting new connections and lets in-flight handlers finish. By default it installs SIGINT/SIGTERM handlers that call `exitEventLoop()` — you only need to wire up your own if you want the explicit 5 s timeout like in your Go example.

```d
import vibe.vibe;
import core.time;
import core.sys.posix.signal;

void handleRequest(HTTPServerRequest req, HTTPServerResponse res) {
    res.writeBody("Hello World");
}

void main() {
    auto settings = new HTTPServerSettings;
    settings.port = 8080;
    settings.setupSignalHandlers = false;   // we take over

    auto router = new URLRouter;
    router.get("/", &handleRequest);

    auto listener = listenHTTP(settings, router); // blocks; runs event loop

    scope(exit) listener.stopListening();

    void onSignal() @safe nothrow {
        logInfo("shutting down");
        listener.stopListening();             // graceful drain, no new conns
        setTimer(5.seconds, &exitEventLoop);  // hard stop after timeout
    }

    setSignalHandler(SIGINT,  &onSignal);
    setSignalHandler(SIGTERM, &onSignal);
}
```

Notes:
- `setSignalHandler` registers via vibe's internal signal-FD, so the delegate runs on the event loop — safe to call vibe APIs from it (unlike a raw POSIX handler).
- If you don't care about the timeout, just leave `setupSignalHandlers = true` (default). Vibe will do the graceful drain on Ctrl+C / SIGTERM for you.
- `exitEventLoop()` is vibe's analogue of forcing `srv.Close()` — it terminates waiting fibers. Call it only after `stopListening()` to keep the drain semantics.
- If you don't want the whole framework, `vibe-http` alone (`import vibe.http.server`) exposes `HTTPListener` the same way.
- For a non-vibe alternative, `serverino` (embedded HTTP server + `mir-ion`) or `handy-httpd` (blocking, thread-per-connection — you'd `accept`-loop and close the listening socket on a signal, similar to the Nim version).

## Side-by-side summary

| Concept | Go | Nim (`asynchttpserver`) | D (`vibe.d`) |
|---|---|---|---|
| Stop accepting | `srv.Shutdown(ctx)` | `server.close()` | `listener.stopListening()` |
| Drain in-flight | built into `Shutdown` | manual (counter/chan) | built into `stopListening` |
| Force close | `srv.Close()` | `server.close()` (hard) | `exitEventLoop()` |
| Timeout | `context.WithTimeout` | manual deadline + `poll()` | `setTimer(5.seconds, &exitEventLoop)` |
| Signal source | `signal.NotifyContext` | `posix.signal` / `setControlCHook` | `setSignalHandler` |
| Block until exit | `<-ctx.Done()` | `waitFor serve()` | `listenHTTP()` runs event loop |

One behavioural note: your Go snippet reassigns `ctx` via `signal.NotifyContext(ctx, ...)`, shadowing the parameter — fine, but if you ever add a SIGTERM you'll want `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)`. Both Nim and D examples above already handle SIGTERM.

---

# Part 2 — Primitives & verdict

List all the essential primitives needed for complete precise web server functionality with timeouts and graceful shutdowns in Go, Nim, and D. Go needs context, goroutines, what else underneath? What are these in Nim and D? Which one is most solid, one way/standard/least experimental?


**deepseek:**

## Primitives for a precise HTTP server with timeouts + graceful shutdown

Below is the full primitive stack, layer by layer, then the mapping to Nim and D, then a verdict.

---

## 1. The language‑agnostic checklist

Every "precise" server (timeouts + graceful shutdown + no leaked connections) needs these 12 conceptual primitives, no matter the language:

| # | Primitive | Why |
|---|---|---|
| 1 | **Listening socket + accept loop** | Kernel hands you connections |
| 2 | **Per‑connection unit of concurrency** | One logical task per request |
| 3 | **Read timeout** (header + body) | Slow‑loris defense |
| 4 | **Write timeout** | Stalled writer defense |
| 5 | **Idle / keep‑alive timeout** | Reclaim idle keep‑alive conns |
| 6 | **Per‑request deadline propagation** | Cancel downstream work when request dies |
| 7 | **Cancellation token / context** | Tell handlers "give up" |
| 8 | **Active‑connection registry** | Know what to drain on shutdown |
| 9 | **Graceful stop: close listener, then drain** | No new conns, finish in‑flight |
| 10 | **Hard‑stop fallback after deadline** | Don't hang forever on shutdown |
| 11 | **Signal → shutdown trigger** | SIGINT/SIGTERM → stop |
| 12 | **Connection state hook** (idle vs active) | Needed to distinguish "drain" from "kill" |

Everything below is how each language supplies these.

---

## 2. Go — the full stack under `http.Server`

You already have `context` and goroutines; here's everything else they lean on.

### Application / API layer
- `http.Handler`, `http.HandlerFunc`, `http.ServeMux`
- `http.Server` — with fields `ReadTimeout`, `ReadHeaderTimeout`, `WriteTimeout`, `IdleTimeout`, `MaxHeaderBytes`
- `http.Server.Shutdown(ctx)` — graceful
- `http.Server.Close()` — hard
- `http.Server.RegisterOnShutdown(fn)` — cleanup hooks
- `http.Server.ConnState` — the **connection state hook** (`StateNew/Active/Idle/Closed`)
- `http.Server.SetKeepAlivesEnabled(false)` — force drain of idle conns
- `http.Server.BaseContext` / `ConnContext` — inject per‑conn context

### Cancellation layer
- `context.Context`, `context.WithCancel`, `WithTimeout`, `WithDeadline`
- `context.Cause` (Go 1.20+)
- `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)`

### Concurrency layer
- `go` (goroutine) — unit of concurrency
- `chan struct{}` — used internally for `doneChan` / idle‑conn broadcast
- `select` — multiplex on `ctx.Done()` + timer + done channels
- `sync.WaitGroup` — `listenerGroup` in `http.Server`
- `sync.Mutex` — `http.Server.mu` protects `activeConn map[*conn]struct{}`
- `sync/atomic` — counters
- `golang.org/x/sync/errgroup` (common, not stdlib) — group handler goroutines

### Timeouts (how `http.Server` actually enforces them)
- `time.Timer`, `time.AfterFunc` — used with `Conn.SetReadDeadline` / `SetWriteDeadline`
- Per‑conn: `conn.rwc.SetReadDeadline(...)` before header read, before body read, before each keep‑alive wait

### Network layer
- `net.Listen("tcp", ":8080")` → `net.Listener` (wraps `accept4`)
- `net.Conn` with `SetReadDeadline` / `SetWriteDeadline`
- `crypto/tls` if TLS

### Signal layer
- `os.Signal`, `signal.Notify`, `syscall.SIGINT` / `SIGTERM`
- Internally: `signal.NotifyContext` spawns a goroutine + unbuffered `chan os.Signal`

### Runtime layer (what actually "underneath" does the work)
- **Goroutine scheduler** (M:N, work‑stealing P's)
- **Netpoller** — `epoll` on Linux, `kqueue` on BSD/macOS, IOCP on Windows. `net.Conn` is a non‑blocking fd; the scheduler parks the goroutine on the netpoller when a `Read`/`Write` would block.
- `runtime.GC` / write barriers for `http.Server`'s maps

### OS layer
- `accept4(2)`, `epoll_ctl(2)`, `setsockopt(2)` (`SO_REUSEADDR`, `TCP_NODELAY`, `SO_KEEPALIVE`), `sigaction(2)`, `setrlimit(RLIMIT_NOFILE)`

### What `Shutdown(ctx)` actually does (so you know exactly what you're relying on)
1. `atomic.StoreInt32(&s.inShutdown, 1)` — reject new conns
2. `s.closeListeners()` — stop accepting
3. `s.SetKeepAlivesEnabled(false)`
4. Iterate `s.activeConn`, `closeIdleConns()` → send `Connection: close`
5. Wait on `s.listenerGroup` (a `sync.WaitGroup`) for handlers to return
6. On `ctx.Done()` (your 5 s), loop again calling `c.rwc.Close()` on every remaining conn — **hard kill**
7. Return `ctx.Err()` if deadline was hit

The whole thing is ~120 lines of stdlib (`net/http/server.go`) and has been stable since Go 1.8.

---

## 3. Nim — mapping the checklist

Nim has **no single equivalent of `http.Server` with all 12 primitives**. You either accept missing pieces or assemble them.

### `std/asynchttpserver` (stdlib, ~toy grade)
| Checklist item | Primitive | Status |
|---|---|---|
| 1 | `newAsyncHttpServer()` + `serve(Port, handler)` | ✅ |
| 2 | `async` proc per request (dispatcher future) | ✅ |
| 3/4 | — | ❌ (no read/write timeout) |
| 5 | — | ❌ |
| 6 | — | ❌ |
| 7 | `Future` (not cancellable in flight) | ⚠️ |
| 8 | — | ❌ |
| 9 | `server.close()` flips `running=false`, closes listener | ⚠️ partial |
| 10 | — | ❌ (manual) |
| 11 | `posix.signal` / `setControlCHook` | ✅ manual |
| 12 | — | ❌ |

### `chronos` (the "serious" async framework)
| Item | Primitive |
|---|---|
| 1 | `HttpServer` / `new` + `serve` |
| 2 | `async`/`await` (chronos futures) |
| 3/4 | **no built‑in per‑request read/write timeout in `HttpServer`** — you attach `sleepAsync(timeout)` + `race`/`withTimeout` per handler |
| 5 | manual keep‑alive timer |
| 6/7 | `CancelledError`, `cancelAndWait`, `withTimeout` |
| 8 | manual `HashSet[AsyncFD]` registry |
| 9 | `server.close()` (drains) |
| 10 | `withTimeout(fut, 5.seconds)` around the drain |
| 11 | `posix.signal` + `addSignalHandler` |
| 12 | manual |

### Other options
- **`httpbeast`** — very fast, **no graceful shutdown**, no timeouts. Wrong tool for this.
- **`jester`** — web framework on `asynchttpserver`; inherits its limits.
- **`chronos/apps/http/httpserver`** — closest thing to Go's model but has less adoption than `httpbeast`/Jester.

### Missing at the language level
- **No `Context` equivalent.** `asyncdispatch` Futures are not cancellable in flight. `chronos` adds `CancelledError` + `cancelAndWait`, but it's framework‑specific, not language‑wide.
- **No deadline propagation type** across APIs (no `context`‑style tree).
- **No standard `Shutdown(ctx)`** — you write it.
- **No connection state hook** — no `ConnState` equivalent in stdlib.

Bottom line: with chronos you can build Go's shape, but every one of items 3–5, 8, 9, 10, 12 you assemble yourself.

---

## 4. D — mapping the checklist (`vibe.d` / `vibe-http`)

D's ecosystem has the **closest semantic match to Go**. `vibe-core` is the event loop; `vibe-http` is `net/http`.

| Checklist item | D primitive | Notes |
|---|---|---|
| 1 | `listenHTTP(settings, router)` → `HTTPListener` (wraps `epoll`/`kqueue`/IOCP) | ✅ |
| 2 | `Task` on a `Fiber` — vibe's unit of concurrency (**≈ goroutine**) | ✅ |
| 3 | `HTTPServerSettings.readTimeout` | ✅ |
| 4 | `HTTPServerSettings.writeTimeout` | ✅ |
| 5 | `HTTPServerSettings.keepAliveTimeout` + `maxRequestsPerConnection` | ✅ |
| 6 | `setTimer`, request‑scoped deadline via `scope(exit)` | ⚠️ manual propagation |
| 7 | `Task.interrupt()` / `Fiber` cancellation, `scope(exit)` cleanup | ⚠️ weaker than `context` |
| 8 | `HTTPListener` tracks conns internally | ✅ |
| 9 | **`listener.stopListening()`** — exactly `Server.Shutdown` semantics | ✅ |
| 10 | `setTimer(5.seconds, &exitEventLoop)` — hard fallback | ✅ |
| 11 | `setSignalHandler(SIGINT/SIGTERM, &handler)` — runs on event loop | ✅ |
| 12 | `HTTPServerRequest` lifecycle; `requestedPath`, `res.onEnd` | partial |

### Underlying stack
- **`vibe-core` Scheduler** — `epoll`/`kqueue`/IOCP event loop, fiber scheduler, timers
- **`vibe.d` signal FD** — signals are delivered as `EventType.signal` inside the loop, so handlers are safe to call vibe APIs from
- `core.time.MonoTime`, `setTimer`, `Timer`
- `std.socket`, `core.sys.posix.signal` for the low‑level bits
- `scope(exit)` / `scope(success)` for cleanup — best‑in‑class in any of the three

### What's missing vs Go
- No `Context` tree with deadline inheritance; deadlines are per‑timer, not propagated.
- `stopListening()` is graceful but does **not** force‑kill after a deadline — you compose that with `exitEventLoop()` yourself (3 lines).
- `Task` cancellation is less ergonomic than `ctx.Done()` select loops.

---

## 5. Side‑by‑side

| Primitive | Go | Nim (chronos) | D (vibe.d) |
|---|---|---|---|
| Concurrency unit | goroutine | `Future` / `async` | `Task` on `Fiber` |
| Cancellation | **`context.Context`** | `CancelledError` + `cancelAndWait` | `Task.interrupt` / `scope(exit)` |
| Deadline propagation | **built into context tree** | manual | manual |
| Read timeout | `ReadTimeout` / `ReadHeaderTimeout` | manual race | `readTimeout` |
| Write timeout | `WriteTimeout` | manual race | `writeTimeout` |
| Idle timeout | `IdleTimeout` | manual | `keepAliveTimeout` |
| Conn state hook | `ConnState` | ❌ | partial (`res.onEnd`) |
| Graceful stop | **`Shutdown(ctx)`** | `close()` + manual drain | **`stopListening()`** |
| Hard stop | `Close()` | `server.close()` | `exitEventLoop()` |
| Signal→shutdown | `signal.NotifyContext` | `posix.signal` | `setSignalHandler` |
| Event loop | netpoller (runtime) | `asyncdispatch` / chronos | vibe‑core Scheduler |
| Batteries included | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |

---

## 6. Which is most solid / standard / least experimental?

**Go — clearly, by a wide margin.**

Reasons, concrete:

1. **`http.Server` is the reference implementation.** `Shutdown(ctx)` semantics are documented, stable since Go 1.8, and exercised by Kubernetes, Docker, Prometheus, etcd, Caddy, Traefik. It is not a library decision you have to justify — it's the default.
2. **Timeouts are struct fields**, not something you race into handlers. You cannot accidentally forget `ReadHeaderTimeout` without it being obvious in review.
3. **`context.Context` is *the* cancellation primitive**, and it is threaded through `database/sql`, `net/http`, gRPC, everything. Nothing in Nim or D comes close to this.
4. **The 12‑item checklist is 9 lines of Go**, most of which is your existing `run()`.
5. **Backwards‑compatibility promise** means the API you write today won't rot.
6. **No third‑party async framework.** `net/http` is stdlib. In Nim you need chronos; in D you need vibe.d. Both are single‑project ecosystems with smaller bus factors.

**D (vibe.d) — second.** It's genuinely production‑used, the primitives (`stopListening`, `readTimeout`, `setSignalHandler`, `scope(exit)`) map cleanly onto the checklist, and the fiber model is the closest to Go's. If you're already in D, this is a fine choice — but it's one framework's design, not a language standard, and D's user base is ~2 orders of magnitude smaller than Go's.

**Nim chronos — third.** Good library, but: cancellation is weak, there's no stdlib `Shutdown(ctx)`, and you write items 3–5/8/9/10/12 yourself. `chronos` `HttpServer` is a real server, but you're holding more rope.

**Nim `std/asynchttpserver` — last.** The docs literally describe it as a basic server; no timeouts, no graceful drain, no connection registry. Treat as a toy for scripts, not for "complete precise" anything.

### If I had to rank by "how many of the 12 primitives are one line of stdlib":

| Rank | Language + stack | Primitives out of 12 you get free |
|---|---|---|
| 1 | **Go `net/http`** | **12 / 12** |
| 2 | D `vibe.d` | 9 / 12 (3–4 need a few lines each) |
| 3 | Nim `chronos` | 5 / 12 |
| 4 | Nim `asynchttpserver` | 2 / 12 |

So: if the goal is *"complete precise web server with timeouts and graceful shutdown, standard, least experimental"* — **Go, with `net/http` + `context` + `signal.NotifyContext`, is the answer.** Nim and D both get there, but in Nim you're building the server; in D you're using a good framework; in Go you're using the platform.

---

# Part 3 — Concurrency & parallelism

What's the default architecture for parallelism and concurrency in each? Is Nim really that far behind Go with solid defaults here, and is D that close? Do Nim and D provide green threads like goroutines and feature parity as in Go, how much harder is to do concurrency and parallelism in D and Nim than in Go?


**deepseek:**

## Concurrency & parallelism architecture: Go vs Nim vs D

The honest short answer: **Go's model is unified; D's is fragmented but capable; Nim's is genuinely behind in one specific way (no stackful green threads in stdlib) and its parallelism/concurrency story is the most "assemble it yourself."** But "how much harder" depends heavily on which 80% or 20% of the problem you're in. Let me lay it out.

---

## 1. Default architecture, per language

### Go — one model, M:N, preemptive

| Layer | What it is |
|---|---|
| Unit | **goroutine** — stackful, ~2 KB initial stack, grows/shrinks |
| Scheduler | **M:N** runtime scheduler. `G` (goroutine) / `M` (OS thread) / `P` (logical CPU, `GOMAXPROCS`) |
| Load balance | Work-stealing run queues per `P` |
| Preemption | **Async preemption** since Go 1.14 — signal-based, can preempt even tight `for {}` loops |
| I/O | Integrated **netpoller** (`epoll`/`kqueue`/IOCP); `conn.Read()` "just blocks" — runtime parks the `G`, no code changes |
| Communication | Channels + `sync` package (Mutex, WaitGroup, atomic, Map) |
| Cancellation | `context.Context` |
| Parallelism | Free — `GOMAXPROCS` threads, scheduler uses all cores by default |
| Race detection | `go build -race`, first-class |

Key point: **there is only one model.** `go f()` works for CPU, I/O, mixed. `database/sql`, `net/http`, gRPC are all built on `context` + goroutines.

### Nim — two disjoint models, neither is M:N

Nim's stdlib offers **two incompatible concurrency stories**, and you must pick per problem:

**A. `std/asyncdispatch` (I/O concurrency)** — like Python `asyncio` / Rust `async`
- Single-threaded event loop (`selectors` = `epoll`/`kqueue`)
- `Future[T]` + `{.async.}` macro transforms procs into **stackless state machines**
- **Cooperative only** — no preemption. Must `await` to yield.
- Function **coloring**: every caller in the chain must be `{.async.}`
- Cannot call blocking code without freezing the entire loop
- `chronos` is a fork adding cancellation (`cancelAndWait`, `withTimeout`), still stackless and single-threaded-per-loop

**B. `std/threadpool` + `std/channels` (parallelism)** — real OS threads
- `--threads:on` required
- `spawn f()` → `FlowVar[T]` → `^fv` to force
- `Channel[T]` for message passing (lock + condvar, not lock-free)
- Fixed pool; no work-stealing; no preemption in the Go sense
- **Cannot `await` a `Future` across threads** — each thread needs its own dispatcher

**Combining A + B is the painful part.** Nim has no supported way to run one event loop across multiple threads. If you need 8 event loops (one per core), you build it yourself.

**Green threads?** **No, not in the Go sense.** Nim's `async` is stackless (coroutine-like), **not** stackful green threads. There is no `Goroutine` equivalent in the stdlib. `weave` (mratsim) was an experimental work-stealing stackful scheduler, but it never reached stdlib and has been dormant.

### D — four models, each good at one thing

| Model | Unit | Backing | Use case |
|---|---|---|---|
| `core.thread` | `Thread` | **OS thread** | Raw threads |
| `core.thread.Fiber` | `Fiber` | **Stackful user-space green thread** — you call `yield()` manually | Roll your own scheduler |
| `std.concurrency` | `spawn` → `Tid` | **OS threads** underneath | Actor-style message passing |
| `std.parallelism` | `taskPool` | **Work-stealing pool** of OS threads | CPU-bound fork/join |
| `vibe-core` (`Task`) | `Task` | **Fiber + event loop**, implicit yield | I/O concurrency, like `asyncio` |

Key facts:
- **`std.parallelism` is genuinely good** — real work-stealing, `parallel(arr, fn)`, `taskPool.submit` → `Task.workForce`, `amap`, `reduce`. This is the closest thing to Go's `P`-stealing outside Go.
- **`std.concurrency`** is *like* Go's channels but **heavier**: `spawn` creates an OS thread, not a fiber. Message passing via `send`/`receive` mailboxes. No `context`, no cancellation.
- **`vibe-core`'s `Task`** is the closest to goroutines — **stackful fibers with implicit yield**. But only inside vibe's event loop.
- **`Fiber`** is stackful like goroutines, but **cooperative** (must call `yield()`) and **you have to schedule it** — no M:N from stdlib.

**Green threads?** **Yes, `Fiber` is stackful like goroutines**, and `vibe-core` puts an M:1 scheduler on top of it (`runTask`, `runWorkerThread`). But this is a *framework* feature, not a *language* feature.

---

## 2. Green-thread feature parity — the honest matrix

| Property | Go goroutine | D `Fiber` / `Task` | Nim `Future` (async) |
|---|---|---|---|
| Stackful | ✅ | ✅ | ❌ stackless |
| Initial stack cost | ~2 KB | ~4–16 KB (tunable) | 0 (state machine) |
| Yield **anywhere** (nested calls) | ✅ | ✅ | ❌ "async colored" |
| Preemptive | ✅ (async signals, 1.14+) | ❌ cooperative | ❌ cooperative |
| M:N scheduler | ✅ built-in | ✅ in vibe-core only | ❌ |
| Call blocking code transparently | ✅ netpoller parks `G` | ❌ blocks the loop | ❌ blocks the loop |
| Cancellation | ✅ `context` | ⚠️ `Task.interrupt()` (vibe only) | ⚠️ chronos only |
| Interop with stdlib I/O | ✅ everywhere | ⚠️ vibe's I/O only (or `Fiber` DIY) | ❌ need `async` wrapper per call |

**The "function coloring" problem** (Rust/Tokio, Python asyncio, Nim `asyncdispatch`) does **not** exist in Go, and it does **not** exist in D's `Fiber`/`vibe` — but it *pervades* Nim async. Any nested call to blocking code must be turned into `{.async.}`, all the way up. In Nim you can't `await` inside a `{.async.}` proc that has a regular callback — the composition surface is smaller than Go's.

---

## 3. "How much harder" — with the same task

**Task: fetch 100 URLs concurrently and wait for all.**

### Go — 1 shape, doesn't care if `fetch` is blocking
```go
var wg sync.WaitGroup
for _, u := range urls {
    wg.Add(1)
    go func(u string) { defer wg.Done(); fetch(u) }(u)
}
wg.Wait()
```

### D (vibe.d) — 1 shape, `fetch` must use vibe's I/O
```d
import vibe.core.task : runTask;
Task[] ts;
foreach (u; urls)
    ts ~= runTask({ fetchVibe(u); });   // fetch must be vibe-aware
foreach (t; ts) t.join();
```

### D (std.parallelism) — for CPU work, blocking is fine
```d
import std.parallelism;
foreach (u; urls.parallel)
    fetchBlocking(u);   // runs on a pool OS thread, blocks OK
```

### Nim (asyncdispatch) — `fetch` must be `{.async.}`, every caller becomes `{.async.}`
```nim
import std/[asyncdispatch, asyncfutures]
var futs: seq[Future[void]]
for u in urls:
    futs.add fetch(u)          # fetch is {.async.}
waitFor all(futs)              # must be inside an async context too
```

### Nim (threads) — OS threads, `FlowVar`
```nim
import std/[threadpool, os]
for u in urls:
    discard spawn fetchBlocking(u)   # returns FlowVar[void]
sync()                                # from threadpool
```

Notice the **line count is similar**. The difference is not "how many lines" — it's **semantic friction**:

| Concern | Go | D vibe | D std.parallelism | D std.concurrency | Nim async | Nim threads |
|---|---|---|---|---|---|---|
| Blocking call inside unit | ✅ transparent | ❌ blocks loop | ✅ | ✅ (OS thread) | ❌ blocks loop | ✅ |
| Cancellation | ✅ `ctx` | ⚠️ `Task.interrupt()` | ❌ | ❌ | ⚠️ chronos only | ❌ |
| Error propagation | ✅ `errgroup` | ✅ per-`Task` | ✅ exceptions | ⚠️ messages | ⚠️ `future.read` | ⚠️ manual |
| Mix concurrency + parallelism | ✅ free | ❌ | ❌ | ❌ | ❌ | ❌ |
| Timeout per unit | `ctx.WithTimeout` | `withTimeout` | manual | manual | `withTimeout` | manual |
| Debug story (stack traces) | ✅ | ✅ | ✅ | ⚠️ mailbox-based | ⚠️ macro-generated | ✅ |

**The 80% that's easy in all three:** "spawn a bunch of units, wait for them all." ~1–3 lines in each language.

**The 20% that diverges — and it's where servers live:**
1. **Calling blocking library code** (SQL driver, disk, TLS handshake) — free in Go/threads, breaks the loop in Nim async / D vibe.
2. **Cancellation** — one `context` in Go; in Nim chronos-only; in D only vibe's `Task`.
3. **Mixing CPU parallelism with I/O concurrency** (e.g. an HTTP server where each handler calls a CPU-bound computation in parallel) — Go: just use a `sync.Pool` + worker goroutines. Nim: two event loops + threads glued. D: vibe + `taskPool`, doable but ugly.
4. **Preemption** — Go: even an infinite-loop handler can be preempted. Nim/D: a `while true {}` inside `async`/`Fiber` **deadlocks the whole loop**.

---

## 4. Is Nim really that far behind? Is D that close?

### Nim — yes, materially behind for servers

Concrete gaps that bite production:
- **No unified model.** `asyncdispatch` and `threadpool` don't compose. There's no `context`-like type that spans both.
- **No stackful coroutines in stdlib.** Async is stackless (Rust/Python-colored).
- **No M:N scheduler.** Single-threaded event loop; you'd write N event loops and shard by thread manually.
- **No preemption.** A blocking call inside a handler freezes the world.
- **No `shutdown(ctx)` primitive** in `asynchttpserver` (already covered).
- **`weave`** — the one serious attempt at a Go-like scheduler — was experimental and is effectively dormant.

Nim **does** have a strong macro system, ORC/ARC memory management, and can call C directly. But the concurrency story is: *pick a library, accept the trade-offs, write the glue*. That's not Go's "one model, all defaults, done."

### D — closer than Nim, but **fragmented**

D's story is better than "far behind," worse than "parity":

**Where D is genuinely close to Go:**
- **`Fiber`** is stackful like goroutines (the other two languages don't have this in stdlib — Nim has no stackful green threads at all).
- **`std.parallelism`** has a real work-stealing task pool — the closest analogue to Go's `P`-queues outside Go.
- **`vibe-core`'s `Task`** gives implicit-yield fiber concurrency, no coloring.
- **`scope(exit)` / `scope(failure)`** is arguably *cleaner* than `defer` + `context` for cleanup.
- **Exceptions** are the error model everywhere, consistently.

**Where D is fragmented vs Go:**
- Four models (`Fiber`, `std.concurrency`, `std.parallelism`, `vibe` `Task`) that **don't unify**. A `Task` is not a `Thread` is not a `FlowVar`.
- **`std.concurrency` spawns OS threads**, not fibers — 8 MB stack, expensive. Go spawns a 2 KB goroutine.
- **No `context` equivalent** at the language level. `Task.interrupt()` is vibe-only.
- **No preemption.** A tight loop in a `Fiber` starves the loop (same as Nim async).
- **`std.concurrency` has no cancellation** at all.
- **Combining vibe's loop with `taskPool`** is possible but you glue it manually.

D's `std.parallelism` is the strongest fragment. D's `vibe` is the closest to Go's ergonomics for servers. But there is no single import that gives you the equivalent of `net/http` + `context` + `signal.NotifyContext`.

---

## 5. Direct scorecard

How far each is from "Go-like default":

| Dimension | Go | D | Nim |
|---|---|---|---|
| **Stackful green thread in stdlib** | ✅ goroutine | ⚠️ `Fiber` (no scheduler) | ❌ stackless only |
| **M:N scheduler** | ✅ built-in, preemptive | ⚠️ in `vibe-core` only | ❌ |
| **One unified model** | ✅ | ❌ four | ❌ two incompatible |
| **Cancellation type** | ✅ `context`, everywhere | ⚠️ per-framework | ⚠️ chronos only |
| **Transparent blocking I/O** | ✅ | ⚠️ only inside vibe | ❌ freezes loop |
| **Parallel CPU from one API** | ✅ `go` | ✅ `parallel`, `taskPool` | ⚠️ `threadpool` (glue) |
| **Race detector** | ✅ `-race` | ❌ | ❌ |
| **Compose concurrency + parallelism** | ✅ | ❌ | ❌ |
| **Production web server in stdlib** | ✅ `net/http` | ⚠️ `vibe-http` (third-party) | ❌ `asynchttpserver` is toy |

**Friction factor, roughly (1 = Go):**

- Go: **1×** — `go`, `select`, `context`, done.
- D (std.parallelism, CPU fork/join): **1–2×** — genuinely clean.
- D (vibe, I/O server): **2–3×** — ergonomic but framework-locked; cancellation weaker.
- D (std.concurrency, actors): **3–5×** — OS threads, mailbox RPC, no cancellation.
- D (mixed vibe + parallelism in one service): **5–10×** — you're writing the glue.
- Nim (asyncdispatch, single event loop): **3–5×** — futures, coloring, no cancellation.
- Nim (threads only, CPU work): **3–4×** — `spawn`/`FlowVar`/`sync`, but OS threads.
- Nim (async **plus** threads in one service, e.g. server with CPU work): **10×+** — separate event loops per thread, no supported cross-thread futures, no unified cancellation.

---

## 6. Bottom line

- **Is Nim far behind Go on concurrency/parallelism defaults?** **Yes** — not for lack of ambition, but because the stdlib only gives you stackless coroutines (single-threaded) and OS threads, with no M:N and no stackful green threads. `weave` was the fix and it didn't ship. You can build good servers in Nim, but you're assembling concurrency machinery, not using it.
- **Is D that close to Go?** **Closer than Nim, yes — but not parity.** D gives you the right *building blocks* (`Fiber`, `parallelism`, `vibe` `Task`) and no coloring problem, but no single unified model, no `context`, no preemption, and no stdlib `Shutdown(ctx)`-equivalent web server. If your problem is "CPU work in parallel," D is great. If it's "long-lived service with mixed I/O + CPU + cancellation," you'll feel the seams — more than Go, less than Nim.
- **Green threads:** only **Go** and **D's `Fiber`/`Task`** are stackful like goroutines. **Nim's async is stackless** (Rust/Python-colored). D still requires a framework (vibe-core) to schedule fibers as an M:N pool; Go's is the runtime.
- **How much harder?** For trivial "spawn N and wait": ~1×. For the 20% that matters — cancellation, mixing parallelism + concurrency in one process, transparent blocking, preemption — **D is ~2–3× harder, Nim async is ~5×, Nim mixed async+threads is ~10×, D mixed vibe+parallelism is ~5–10×.** None of it is impossible; Go just made *all* of it one model, which is why every serious server in Go looks the same and every serious server in D or Nim looks different.


