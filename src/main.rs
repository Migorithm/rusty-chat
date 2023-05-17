use async_std::{
    io::BufReader, //3
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*, //1
    task,       //2 task module roughly corresponds to the std::thread module,
};
use futures::{channel::mpsc, select, FutureExt, SinkExt};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
// Sending messages
type Sender<T> = mpsc::UnboundedSender<T>; // Don't care about backpressure for now
type Receiver<T> = mpsc::UnboundedReceiver<T>;

#[derive(Debug)]
pub enum Void {}

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

async fn accept_loop(addr: impl ToSocketAddrs) -> Result<()> {
    let listener: TcpListener = TcpListener::bind(addr).await?;

    // Create channel
    let (sender_to_broker, receiving_end_on_broker) = mpsc::unbounded();

    // * 'receiving end' is sent to 'broker_loop' which is dispatcher that controls receiver.
    // * This receiver actor can make local decisions, such as maintaining local state,
    // * creating more Actors, sending more messages, designating how to respond to the next message.

    // ? Why only 'receiving end'?
    // * That's because all the senders will be sent to connection_loop
    // * within which the sender will send Event which is either NewPeer or Message
    let _call_to_broker = task::spawn(broker_loop(receiving_end_on_broker));

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
        spawn_and_log_error(connection_loop(sender_to_broker.clone(), stream));
    }
    drop(sender_to_broker);
    _call_to_broker.await?;
    Ok(())
}

async fn connection_loop(mut sender_to_broker: Sender<Event>, stream: TcpStream) -> Result<()> {
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

    // * In the reader, we create a shutdown sender whose only purpose is to get dropped
    let (_shutdown_sender, shutdown_receiver) = mpsc::unbounded::<Void>();

    // * On Login, we notify the broker.
    // * Note that we used unwrap() because broker should outlive all the clients and if it doesn't, we have to raise panic.
    // * Send NewPeer doesn't necessarily mean that it will be newly registered as there is checking logic in broker_loop
    sender_to_broker
        .send(Event::NewPeer {
            name: name.clone(),
            stream: Arc::clone(&stream),
            shutdown: shutdown_receiver,
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
        sender_to_broker
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
    mut messages: &mut Receiver<String>,
    stream: Arc<TcpStream>,
    mut shutdown: Receiver<Void>,
) -> Result<()> {
    let mut stream = &*stream;

    loop {
        // * futures::select
        // * https://docs.rs/futures/latest/futures/macro.select.html
        select! {
            // * Function fuse() is used to turn any Stream into a FusedStream
            // * fuse() creates a stream that ends after the first None
            // * This is used for fusing a stream such that poll_next will never again be called once it has finished.
            msg = messages.next().fuse() => match msg{
                Some(msg) => stream.write_all(msg.as_bytes()).await?,
                None => break
            },
            void = shutdown.next().fuse() => match void{
                Some(void)=>match void{}, // ! in writer side, we decide not to process message if shutdown message comes first.
                                          // ! select! polls multiple futures and streams simultaneously, executing the branch for the future that finishes first.
                None => break,
            }
        }
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
pub enum Event {
    NewPeer {
        name: String,
        stream: Arc<TcpStream>,
        shutdown: Receiver<Void>, // pass shutdown channel to the writer task
    },
    Message {
        from: String,
        to: Vec<String>,
        msg: String,
    },
}

async fn broker_loop(mut events: Receiver<Event>) -> Result<()> {
    let (disconnect_sender, mut disconnect_receiver) =
        mpsc::unbounded::<(String, Receiver<String>)>();

    // * An actor holds its own private state and it can decide how to process the next message
    // * based on that state. Internal state changes only after it receives message.
    // * Every actor has its mailbox which is simmilar to message queue
    // * Conceptually, an actor can handle one message at a time
    let mut peers: HashMap<String, Sender<String>> = HashMap::new();

    // ? How do we handle disconnections?
    // ? Simply deleting user from peers map would be wrong. because..
    // * If both read and write fail, we'll remove the peer twice,
    // * but it can be the case that the peer reconnected between the two failures!
    // ! To fix this, we will only remove the peer when the write side finishes.
    // ! we need to add an ability to signal shutdown for the writer task.
    // * 1. shutdown: Receiver<()> channel.
    // * 2. just drop the sender

    let mut events = events.fuse();
    loop {
        // ! we are not sure which event comes first.
        // ! so.. select! is used which polls multiple futures and streams simultaneously, executing the branch for the future that finishes first.
        let event = select! {
            event = events.next().fuse() => match event{
                None=>break, // * Exit when all readers exit
                Some(event) => event
            },
            disconnect = disconnect_receiver.next().fuse() => {
                let (name, _pending_messages) = disconnect.unwrap();
                assert!(peers.remove(&name).is_some());
                continue
            },
        };

        match event {
            Event::Message { from, to, msg } => {
                for addr in to {
                    if let Some(peer) = peers.get_mut(&addr) {
                        let msg = format!("from {}: {}\n", from, msg);
                        peer.send(msg).await?
                    }
                }
            }
            Event::NewPeer {
                name,
                stream,
                shutdown,
            } => {
                match peers.entry(name.clone()) {
                    Entry::Occupied(..) => (),
                    Entry::Vacant(entry) => {
                        // To handler a new peer, we first register it in the peer's map
                        let (client_sender, mut client_receiver) = mpsc::unbounded();
                        entry.insert(client_sender);

                        let mut disconnect_sender = disconnect_sender.clone();
                        spawn_and_log_error(async move {
                            let res =
                                connection_writer_loop(&mut client_receiver, stream, shutdown)
                                    .await;

                            disconnect_sender
                                .send((name, client_receiver))
                                .await
                                .unwrap(); // * we can safely unwrap because this sender outlives receiver in connection_writer_loop
                            res
                        });
                    }
                }
            }
        }
    }

    drop(peers);
    drop(disconnect_sender); //* Finally, we close and drain the disconnections channel which isn't fully drained  in the main loop as broker itself holds them.

    while let Some((_name, _pending_messages)) = disconnect_receiver.next().await {}

    Ok(())
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

fn main() {}
