//! Sends messages from topic to [`TcpStream`] of the subscriber
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::broadcast::Sender};
pub async fn subscriber(mut socket: TcpStream, tx: Sender<Vec<u8>>) {
    let mut rx = tx.subscribe();
    loop {
        let message = match rx.recv().await {
            Ok(msg) => msg,
            Err(_) => break,
        };
        let _ = socket.write_all(&message).await;
    }
}
