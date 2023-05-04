use async_std::{
    io::BufReader, //3
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*, //1
    task,       //2 task module roughly corresponds to the std::thread module,
};
use futures::{channel::mpsc, SinkExt};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
// Sending messages
type Sender<T> = mpsc::UnboundedSender<T>; // Don't care about backpressure for now
type Receiver<T> = mpsc::UnboundedReceiver<T>;

fn run() -> Result<()> {
    //! Here, accept_loop is root function listening on 127.0.0.1:8080
    let fut = accept_loop("127.0.0.1:8080");

    /*
     *  In Rust, unlike other languages, calling an async function doesn't run any code
     *  async fn only construct futures. We should use .await in `non-async` function
     * in this case, we use task::block_on to execute future
     */
    task::block_on(fut)
}

fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    /*
    * Could have been the following
    ! let handle = task::spawn(connection_loop(stream));
    ! handle.await?
    * But .await waits until the client finishes nullifying the meaning of async
    * Plus, it a client encounters an IOError, the whole server will exit, meaning that
    * One user can bring down the whole chat service.
     */

    task::spawn(async move {
        if let Err(e) = fut.await {
            eprintln!("{}", e)
        }
    })
}

async fn accept_loop(addr: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    // Create channel
    let (broker_sender, broker_receiver) = mpsc::unbounded();

    // * 'receiving end' is sent to 'broker_loop' which is dispatcher the controls receiver.
    // * This receiver actor can make local decisions, such as maintaining local state,
    // * creating more Actors, sending more messages, designating how to respond to the next message.

    // ? Why only 'receiving end'?
    // * That's because all the senders will be sent to connection_loop
    // * within which the sender will send Event which is either NewPeer or Message
    let broker = task::spawn(broker_loop(broker_receiver));

    let mut incoming = listener.incoming();

    /*
     * If it were for std, it would've been for stream in listener.incoming(){}
     * But there is no support for async for yet. Therefore, we use 'while let Some(s) = incoming.next().await' syntax
     */
    while let Some(stream) = incoming.next().await {
        /*
         * 1. split incoming TcpStream on \n and decode bytes as utf-8
         * 2. interpret the first line as a login
         * 3. parse the rest of the lines as a login: message
         */

        let stream = stream?; // ! message is shared between receiver and sender actor
        println!("Accepting from: {}", stream.peer_addr()?);

        /*
         * use task::spawn function to spawn an independent task for working with each client
         * that is, after accepting the client, `accept_loop` starts waiting for the next one.
         * Correct way to handle client errors in this case is log them and continue serving
         * other client so helper function `spawn_and_log_error` is introduced.
         */
        spawn_and_log_error(connection_loop(broker_sender.clone(), stream));
    }
    Ok(())
}

async fn connection_loop(mut broker: Sender<Event>, stream: TcpStream) -> Result<()> {
    // * Inside connection_loop,
    // * we need to wrap TcpStream into an Arc,
    // * to be able to share it with the connection_writer_loop
    let stream = Arc::new(stream);
    let reader = BufReader::new(&*stream);
    let mut lines = reader.lines();

    let name = match lines.next().await {
        None => Err("Peer disconnected immediately")?,
        Some(line) => line?,
    };

    // * On Login, we notify the broker.
    // * Note that we used unwrap() because broker should outlive all the clients and if it doesn't, we have to raise panic.
    // * Send NewPeer doesn't necessarily mean that it will be newly registered as there is checking logic in broker_loop
    broker
        .send(Event::NewPeer {
            name: name.clone(),
            stream: Arc::clone(&stream),
        })
        .await
        .unwrap();

    while let Some(line) = lines.next().await {
        let line = line?;

        // * Finally, we parse each line into a list of destination logins and the message itself.
        let (dest, msg) = match line.find(':') {
            None => continue,
            Some(idx) => (&line[..idx], line[idx + 1..].trim()),
        };
        let dest = dest
            .split(',')
            .map(|name| name.trim().to_string())
            .collect::<Vec<String>>();
        let msg = msg.to_string();

        // * Forward parsed messages to the broker, in case it passed parsing logic above
        broker
            .send(Event::Message {
                from: name.clone(),
                to: dest,
                msg,
            })
            .await
            .unwrap();
    }

    Ok(())
}

async fn connection_writer_loop(
    mut messages: Receiver<String>,
    stream: Arc<TcpStream>,
) -> Result<()> {
    let mut stream = &*stream;
    while let Some(msg) = messages.next().await {
        stream.write_all(msg.as_bytes()).await?;
    }
    Ok(())
}

/*
* Messages from connecetion_loop should flow into the connection_writer_loop.
* To do that, we should maintain a peers: HashMap<String, Sender<String>> which
* allows a client to find destination channels
*

 */

// Broker task should handle two types: a message and arrival of a new peer
#[derive(Debug)]
enum Event {
    NewPeer {
        name: String,
        stream: Arc<TcpStream>,
    },
    Message {
        from: String,
        to: Vec<String>,
        msg: String,
    },
}

async fn broker_loop(mut events: Receiver<Event>) -> Result<()> {
    // * An actor holds its own private state and it can decide how to process the next message
    // * based on that state. Internal state changes only after it receives message.
    // * Every actor has its mailbox which is simmilar to message queue
    // * Conceptually, an actor can handle one message at a time
    let mut peers: HashMap<String, Sender<String>> = HashMap::new();

    while let Some(event) = events.next().await {
        match event {
            Event::Message { from, to, msg } => {
                for addr in to {
                    if let Some(peer) = peers.get_mut(&addr) {
                        let msg = format!("from {}: {}\n", from, msg);
                        peer.send(msg).await?
                    }
                }
            }
            Event::NewPeer { name, stream } => {
                match peers.entry(name) {
                    Entry::Occupied(..) => (),
                    Entry::Vacant(entry) => {
                        // To handler a new peer, we first register it in the peer's map
                        let (client_sender, client_receiver) = mpsc::unbounded();
                        entry.insert(client_sender);

                        // ! And then  spawn a dedicated task(another actor) to actually write the messages to the socekt
                        spawn_and_log_error(connection_writer_loop(client_receiver, stream));
                    }
                }
            }
        }
    }

    Ok(())
}

fn main() {}
