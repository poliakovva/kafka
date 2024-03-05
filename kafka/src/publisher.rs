//! Sends messages from [`TcpStream`] of the publisher to the topic
use std::net::SocketAddr;

use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, Error, ErrorKind},
    net::TcpStream,
    sync::broadcast::Sender,
};
/// # Message template
/// All recieved messages from the connection should be a correct json-message with attribute "message"
/// # Errors
/// Function will return Err if:
/// * Failed to parse message as json
/// * Incorrect message template
pub async fn publisher(
    mut socket: TcpStream,
    sender: Sender<Vec<u8>>,
    peer_addr: SocketAddr,
) -> io::Result<()> {
    println!("recieved publisher"); //todo info!
    let (mut socket_read, mut socket_write) = socket.split();
    let mut reader = BufReader::new(&mut socket_read);
    loop {
        let mut buffer: Vec<u8> = vec![];
        reader.read_until(b'\n', &mut buffer).await.unwrap();
        buffer.pop();
        let read_json = serde_json::from_slice(&buffer);
        let message: serde_json::Value;
        match read_json {
            Ok(msg) => message = msg,
            Err(_) => {
                let _ = socket_write
                    .write_all(br#"{"error": "received not valid json"}"#)
                    .await;
                println!(
                    "Failed to parse message from publisher {}: {:#?}, closing connection",
                    peer_addr, &buffer
                );

                return Err(Error::new(ErrorKind::Other, "oh no!"));
                //todo:
            }
        }
        match message.get("message") {
            Some(_) => {}
            None => {
                let _ = socket
                    .write_all(br#"{"error": "received not valid json"}"#)
                    .await;
                println!(
                    "Failed to parse message from publisher {}: {:#?}, closing connection",
                    peer_addr, &buffer
                );

                return Err(Error::new(ErrorKind::Other, "oh no!"));
            }
        };

        let _ = sender.send(buffer);
    }
}
