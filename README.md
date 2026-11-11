# Threaded TCP Chat Server

A small, dependency-free chat server written in Rust using only the standard library. Clients connect over plain TCP (for example with `nc` or `telnet`), pick a nickname, and chat with everyone else in a single shared room.

## Features

- One thread per connection, plus a listener thread and a background status thread
- Broadcast chat, private messages, nicknames, and a user list
- Graceful shutdown: press ENTER in the server terminal and all threads stop cleanly
- Connection limit (64 clients) with a "server full" message for extras
- Finished connection threads are joined and cleaned up as the server runs
- Per-line size limit and write timeouts so one misbehaving client can't exhaust memory or stall everyone else
- No external crates

## Requirements

- Rust 1.61 or newer (uses `JoinHandle::is_finished` and `Vec::retain_mut`)

## Setup

```sh
cargo new chat-server
cd chat-server
# replace src/main.rs with the provided main.rs
cargo run --release
```

The server listens on `0.0.0.0:9999` and prints:

```
Starting server...
Listening on 0.0.0.0:9999
Press ENTER to shut down the server.
```

## Connecting

From one or more other terminals:

```sh
nc localhost 9999
```

You'll be assigned a nickname like `guest1` and greeted with a welcome message. Anything you type that doesn't start with `/` is sent to every other connected user as `[nick] message`.

## Commands

| Command | Description |
| --- | --- |
| `/help` | Show the list of commands |
| `/nick <name>` | Change your nickname (1-16 characters: letters, digits, underscore; must be unique, case-insensitive) |
| `/list` | List connected users |
| `/msg <nick> <text>` | Send a private message |
| `/quit` or `/exit` | Disconnect |

Example session:

```
Welcome, guest1! Type /help for commands.
/nick alice
* guest1 is now known as alice
hello everyone
[bob] hi alice!
/msg bob psst
[you -> bob] psst
```

## Shutting down

Press **ENTER** in the terminal running the server. The server then:

1. Sets a shared shutdown flag.
2. Stops accepting new connections.
3. Tells connected clients it is shutting down and closes their sockets.
4. Joins every thread and exits.

Closing stdin (for example Ctrl+D) also triggers shutdown.

## Configuration

Settings are constants at the top of `main.rs`:

| Constant | Default | Meaning |
| --- | --- | --- |
| `ADDR` | `0.0.0.0:9999` | Address and port to bind |
| `MAX_CLIENTS` | `64` | Maximum simultaneous connections |
| `MAX_LINE_LEN` | `1024` | Maximum bytes per incoming line; longer lines disconnect the client |
| `READ_TIMEOUT` | `200 ms` | How often an idle handler wakes up to check for shutdown |
| `WRITE_TIMEOUT` | `2 s` | Maximum time a write to one client may block |

Binding to `0.0.0.0` exposes the server on all network interfaces. Use `127.0.0.1:9999` for local-only testing.

## How it works

```
main thread ──── waits for ENTER, then sets the shutdown flag
├── listener thread ──── accepts connections (non-blocking, polled every 100 ms)
│   ├── t1 ──── handler for client 1
│   ├── t2 ──── handler for client 2
│   └── ...
└── background thread ──── prints a client count every 10 seconds
```

- **Shared state:** connected clients live in an `Arc<Mutex<Vec<Client>>>`. Each entry holds an id, a nickname, and a cloned `TcpStream` used for writing.
- **Reading:** each handler reads lines with a short read timeout, so it can check the shutdown flag regularly. Partial lines stay buffered between timeouts.
- **Writing:** all writes to a client go through the shared list while holding the lock, so messages from different threads never interleave on one socket. The write timeout bounds how long a slow client can hold up a broadcast. Clients whose writes fail are dropped from the list.
- **Cleanup:** the listener joins finished handler threads on every loop, which also surfaces any handler panics in the server log.

## Known limitations

- Single chat room, no authentication, no encryption (traffic is plain text).
- One OS thread per client, which is fine for tens or hundreds of clients but not thousands.
- A broadcast holds the client-list lock while writing, so a slow client can delay others for up to the write timeout.
- Invalid UTF-8 input disconnects the client.

## Ideas for extending it

- Replace per-client locking with an `mpsc` channel and a writer thread per client, so slow clients never block broadcasts
- Add rooms or channels
- Handle Ctrl+C with the `ctrlc` crate
- Structured logging with `tracing` and configuration via `clap` or environment variables
- TLS with `rustls`
- Rewrite on `tokio` for large numbers of connections
