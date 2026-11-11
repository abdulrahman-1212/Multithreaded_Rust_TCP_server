use std::io::{self, BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, MutexGuard,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const ADDR: &str = "0.0.0.0:9999";
const MAX_CLIENTS: usize = 64;
const MAX_LINE_LEN: u64 = 1024;
const READ_TIMEOUT: Duration = Duration::from_millis(200);
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

struct Client {
    id: usize,
    nick: String,
    stream: TcpStream,
}

type Clients = Arc<Mutex<Vec<Client>>>;

/// Lock the client list, recovering from poisoning (a panicked handler
/// shouldn't take the whole server down).
fn lock(clients: &Clients) -> MutexGuard<'_, Vec<Client>> {
    clients.lock().unwrap_or_else(|e| e.into_inner())
}

/// Send to everyone except `skip`. Clients whose socket fails (or times out
/// thanks to the write timeout) are dropped from the list.
fn broadcast(clients: &Clients, skip: Option<usize>, msg: &str) {
    let mut list = lock(clients);
    list.retain_mut(|c| Some(c.id) == skip || c.stream.write_all(msg.as_bytes()).is_ok());
}

/// Send to one client by id. Returns false if the client wasn't found or the write failed.
fn send_to(clients: &Clients, id: usize, msg: &str) -> bool {
    let mut list = lock(clients);
    match list.iter_mut().find(|c| c.id == id) {
        Some(c) => c.stream.write_all(msg.as_bytes()).is_ok(),
        None => false,
    }
}

fn valid_nick(s: &str) -> bool {
    !s.is_empty() && s.len() <= 16 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn main() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clients: Clients = Arc::new(Mutex::new(Vec::new()));

    // Thread 1: TCP listener
    let t1 = {
        let shutdown = Arc::clone(&shutdown);
        let clients = Arc::clone(&clients);

        thread::Builder::new()
            .name("listener".to_owned())
            .spawn(move || run_listener(shutdown, clients))
            .unwrap()
    };

    // Thread 2: Background activity
    let t2 = {
        let shutdown = Arc::clone(&shutdown);
        let clients = Arc::clone(&clients);

        thread::Builder::new()
            .name("background".to_owned())
            .spawn(move || {
                let mut ticks = 0u32;
                while !shutdown.load(Ordering::Acquire) {
                    // Sleep in short steps so shutdown is noticed quickly.
                    thread::sleep(Duration::from_millis(100));
                    ticks += 1;
                    if ticks % 100 == 0 {
                        println!("[status] {} client(s) connected", lock(&clients).len());
                    }
                }
                println!("Background thread stopped");
            })
            .unwrap()
    };

    // Main thread: wait for ENTER (or EOF on stdin).
    println!("Press ENTER to shut down the server.");
    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);

    println!("Shutdown initiated...");
    shutdown.store(true, Ordering::Release);

    t1.join().expect("Listener thread panicked");
    t2.join().expect("Background thread panicked");

    println!("Server shut down successfully.");
}

fn run_listener(shutdown: Arc<AtomicBool>, clients: Clients) {
    println!("Starting server...");

    let listener = match TcpListener::bind(ADDR) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to bind {}: {}", ADDR, e);
            shutdown.store(true, Ordering::Release);
            return;
        }
    };
    listener.set_nonblocking(true).unwrap();
    println!("Listening on {}", ADDR);

    let mut connections: Vec<JoinHandle<()>> = Vec::new();
    let mut next_id = 0usize;

    while !shutdown.load(Ordering::Acquire) {
        reap_finished(&mut connections);

        match listener.accept() {
            Ok((mut stream, addr)) => {
                if shutdown.load(Ordering::Acquire) {
                    break;
                }

                if connections.len() >= MAX_CLIENTS {
                    println!("Rejecting {:?}: server full", addr);
                    let _ = stream.write_all(b"Server full, try again later.\n");
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                }

                next_id += 1;
                println!("Received connection from {:?}", addr);

                match spawn_session(next_id, stream, addr, Arc::clone(&shutdown), Arc::clone(&clients)) {
                    Ok(handle) => connections.push(handle),
                    Err(e) => eprintln!("Failed to spawn handler: {}", e),
                }
            }

            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(100));
            }

            // Transient errors: log and keep serving.
            Err(e) if matches!(e.kind(), ErrorKind::Interrupted | ErrorKind::ConnectionAborted) => {
                eprintln!("Transient accept error: {}", e);
            }

            Err(e) => {
                eprintln!("Fatal accept error: {}", e);
                shutdown.store(true, Ordering::Release);
                break;
            }
        }
    }

    println!("Listener shutting down...");

    for connection in connections {
        if let Err(e) = connection.join() {
            eprintln!("Connection thread panicked: {:?}", e);
        }
    }

    println!("All connection handlers stopped");
}

/// Join and remove handlers that have already finished, reporting panics.
fn reap_finished(connections: &mut Vec<JoinHandle<()>>) {
    let mut i = 0;
    while i < connections.len() {
        if connections[i].is_finished() {
            let handle = connections.swap_remove(i);
            if let Err(e) = handle.join() {
                eprintln!("Connection thread panicked: {:?}", e);
            }
        } else {
            i += 1;
        }
    }
}

fn spawn_session(
    id: usize,
    stream: TcpStream,
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    clients: Clients,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new().name(format!("t{}", id)).spawn(move || {
        println!("Handling connection from {:?}", addr);

        if let Err(e) = session(id, &stream, addr, &shutdown, &clients) {
            println!("Client {:?} setup/IO error: {}", addr, e);
        }

        let _ = stream.shutdown(Shutdown::Both);
        println!("Connection handler {} stopped", id);
    })
}

fn session(
    id: usize,
    stream: &TcpStream,
    addr: SocketAddr,
    shutdown: &AtomicBool,
    clients: &Clients,
) -> io::Result<()> {
    // Accepted sockets may inherit non-blocking mode on some platforms.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    // Socket options are shared with clones, so this also bounds broadcast writes.
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
    stream.set_nodelay(true)?;

    let mut nick = format!("guest{}", id);

    // All fallible setup is done; register the client.
    lock(clients).push(Client {
        id,
        nick: nick.clone(),
        stream: stream.try_clone()?,
    });

    send_to(
        clients,
        id,
        &format!("Welcome, {}! Type /help for commands.\n", nick),
    );
    broadcast(clients, Some(id), &format!("* {} joined\n", nick));

    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    while !shutdown.load(Ordering::Acquire) {
        // `take` caps how much one line can buffer, so a client can't grow memory unboundedly.
        match (&mut reader).take(MAX_LINE_LEN).read_line(&mut line) {
            Ok(0) => break, // client closed the connection
            Ok(_) => {
                if !line.ends_with('\n') {
                    if line.len() as u64 >= MAX_LINE_LEN {
                        send_to(clients, id, "Line too long, disconnecting.\n");
                        break;
                    }
                    continue; // partial data; wait for the rest
                }

                let text = line.trim().to_owned();
                line.clear();

                if text.is_empty() {
                    continue;
                }

                if let Some(cmd) = text.strip_prefix('/') {
                    if !handle_command(cmd, id, &mut nick, clients) {
                        break;
                    }
                } else {
                    println!("[{}] {}", nick, text);
                    broadcast(clients, Some(id), &format!("[{}] {}\n", nick, text));
                }
            }
            // Timeout: partial data stays in `line`; loop around and re-check shutdown.
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => {
                println!("Client {:?} error: {}", addr, e);
                break;
            }
        }
    }

    if shutdown.load(Ordering::Acquire) {
        send_to(clients, id, "Server is shutting down. Bye!\n");
    }

    // Unregister; only announce if we're still in the list (broadcast may have dropped us).
    let was_registered = {
        let mut list = lock(clients);
        let before = list.len();
        list.retain(|c| c.id != id);
        list.len() != before
    };
    if was_registered {
        broadcast(clients, None, &format!("* {} left\n", nick));
    }

    Ok(())
}

/// Handle a slash command. Returns false if the client should disconnect.
fn handle_command(cmd: &str, id: usize, nick: &mut String, clients: &Clients) -> bool {
    let mut parts = cmd.splitn(2, char::is_whitespace);
    let name = parts.next().unwrap_or("");
    let arg = parts.next().unwrap_or("").trim();

    match name {
        "help" => {
            send_to(
                clients,
                id,
                "Commands:\n  /nick <name>        change your nickname\n  /list               list connected users\n  /msg <nick> <text>  private message\n  /quit               disconnect\n",
            );
        }

        "nick" => {
            if !valid_nick(arg) {
                send_to(clients, id, "Nick must be 1-16 chars: letters, digits, underscore.\n");
                return true;
            }

            // Check-and-set under one lock so two clients can't grab the same nick.
            let result = {
                let mut list = lock(clients);
                if list.iter().any(|c| c.id != id && c.nick.eq_ignore_ascii_case(arg)) {
                    Err(())
                } else {
                    if let Some(me) = list.iter_mut().find(|c| c.id == id) {
                        me.nick = arg.to_owned();
                    }
                    Ok(())
                }
            };

            match result {
                Ok(()) => {
                    let old = std::mem::replace(nick, arg.to_owned());
                    broadcast(clients, None, &format!("* {} is now known as {}\n", old, nick));
                }
                Err(()) => {
                    send_to(clients, id, "That nick is taken.\n");
                }
            }
        }

        "list" => {
            let names: Vec<String> = lock(clients).iter().map(|c| c.nick.clone()).collect();
            send_to(
                clients,
                id,
                &format!("{} user(s): {}\n", names.len(), names.join(", ")),
            );
        }

        "msg" => {
            let mut p = arg.splitn(2, char::is_whitespace);
            let target = p.next().unwrap_or("");
            let body = p.next().unwrap_or("").trim();

            if target.is_empty() || body.is_empty() {
                send_to(clients, id, "Usage: /msg <nick> <text>\n");
                return true;
            }

            let target_id = lock(clients)
                .iter()
                .find(|c| c.nick.eq_ignore_ascii_case(target))
                .map(|c| c.id);

            match target_id {
                Some(tid) if send_to(clients, tid, &format!("[{} -> you] {}\n", nick, body)) => {
                    send_to(clients, id, &format!("[you -> {}] {}\n", target, body));
                }
                _ => {
                    send_to(clients, id, &format!("No such user: {}\n", target));
                }
            }
        }

        "quit" | "exit" => {
            send_to(clients, id, "Goodbye!\n");
            return false;
        }

        other => {
            send_to(clients, id, &format!("Unknown command: /{} (try /help)\n", other));
        }
    }

    true
}