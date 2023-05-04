use async_std::{
    io::BufReader, //3
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*, //1
    task,       //2 task module roughly corresponds to the std::thread module,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>; // 4

async fn accept_loop(addr: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();

    /*
     * If it were for std, it would've been for stream in listener.incoming(){}
     * But there is no support for async for yet. Therefore, we use 'while let Some syntax'
     */
    while let Some(stream) = incoming.next().await {
        /*
         * 1. split incoming TcpStream on \n and decode bytes as utf-8
         * 2. interpret the first line as a login
         * 3. parse the rest of the lines as a login: message
         */

        let stream = stream?;
        println!("Accepting from: {}", stream.peer_addr()?);

        /*
         * use task::spawn function to spawn an independent task for working with each client
         * that is, after accepting the client, `accept_loop` starts waiting for the next one.
         * Correct way to handle client errors in this case is log them and continue serving
         * other client so helper function `spawn_and_log_error` is introduced.
         */
        spawn_and_log_error(connection_loop(stream));
    }
    Ok(())
}

async fn connection_loop(stream: TcpStream) -> Result<()> {
    let reader = BufReader::new(&stream);
    let mut lines = reader.lines();

    /*
     * login1, login2, ... loginN: message
     */
    let name = match lines.next().await {
        None => Err("Peer disconnected immediately")?,
        Some(line) => line?,
    };
    println!("name={}", name);
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
    }

    Ok(())
}

fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = fut.await {
            eprintln!("{}", e)
        }
    })
}

fn run() -> Result<()> {
    let fut = accept_loop("127.0.0.1.:8080");

    /*
     *  In Rust, unlike other languages, calling an async function doesn't run any code
     *  async fn only construct futures. We should use .await in `non-async` function
     * in this case, we use task::block_on to execute future
     */
    task::block_on(fut)
}

fn main() {
    println!("Hello, world!");
}
