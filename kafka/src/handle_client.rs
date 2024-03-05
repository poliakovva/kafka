//! Handles recieved connections.

use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, Error, ErrorKind},
    net::TcpStream,
    sync::{
        broadcast::{self, Sender},
        Mutex,
    },
};

use crate::{publisher::publisher, subscriber::subscriber};

/// Get a copy of [`Sender<Vec<u8>>`] for specified topic and hand it over to the [`publisher`] function.
/// # Errors
/// Function will return Err only if it's forwarded from [`publisher`]
pub async fn handle_publisher(
    message_topic: String,
    socket: TcpStream,
    peer_addr: SocketAddr,
    sender_map: Arc<Mutex<HashMap<String, Sender<Vec<u8>>>>>,
) -> io::Result<()> {
    println!(
        "for topic {} connected publisher with ip {}",
        message_topic, peer_addr
    );
    let mut sender_lock = sender_map.lock().await;
    let tx: Sender<Vec<u8>>;
    if sender_lock.contains_key(&message_topic) {
        tx = sender_lock.get(&message_topic).unwrap().clone();
    } else {
        (tx, _) = broadcast::channel(4096);
        sender_lock.insert(message_topic, tx.clone());
    }
    drop(sender_lock);
    let _ = match publisher(socket, tx, peer_addr).await {
        Ok(_) => Ok::<(), Error>(()),
        Err(err) => return Err(err),
    };
    Ok(())
}
///Get a copy of [`Sender<Vec<u8>>`] for specified topic and hand it over to the [`subscriber`] function.
pub async fn handle_subscriber(
    message_topic: String,
    socket: TcpStream,
    peer_addr: SocketAddr,
    sender_map: Arc<Mutex<HashMap<String, Sender<Vec<u8>>>>>,
) {
    println!(
        "for topic {} connected subscriber with ip {}",
        &message_topic, peer_addr
    );

    let mut sender_lock = sender_map.lock().await;

    let tx: Sender<Vec<u8>>;
    if sender_lock.contains_key(&message_topic) {
        tx = sender_lock.get(&message_topic).unwrap().clone();
    } else {
        (tx, _) = broadcast::channel(4096);
        sender_lock.insert(message_topic, tx.clone());
    }
    drop(sender_lock);
    subscriber(socket, tx).await;
}
/// Connection handler for given [`TcpStream`] socket.
/// If recieved correct json message,
/// function hands over the socket to [`handle_publisher`] or [`handle_subscriber`]
/// # Message template
/// First message of connection should be a correct json-message with two attributes:
/// * "method" - either "publisher" or "subscriber"
/// * "topic" - name of the topic
/// # Errors
/// Function will return Err if:
/// * Failed to parse message as json
/// * Incorrect message template ("method", "topic")
/// * Err forwarded from [`handle_publisher`] or [`handle_subscriber`]
pub async fn handle_client(
    mut socket: TcpStream,
    peer_addr: SocketAddr,
    sender_map: Arc<Mutex<HashMap<String, Sender<Vec<u8>>>>>,
) -> io::Result<()> {
    let mut buffer: Vec<u8> = vec![];
    let mut reader = BufReader::new(&mut socket);
    reader.read_until(b'\n', &mut buffer).await.unwrap();
    buffer.pop();
    let read_json = serde_json::from_slice(&buffer);
    let message: serde_json::Value;
    match read_json {
        Ok(msg) => message = msg,
        Err(_) => {
            let _ = socket
                .write_all(br#"{"error": "received not valid json"}"#)
                .await;
            println!(
                "Failed to parse message from publisher {}: {:#?}, closing connection",
                peer_addr, &buffer
            );

            return Err(Error::new(ErrorKind::Other, "oh no!"));
        }
    }
    let message_method = match message.get("method") {
        Some(msg) => msg,
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
    let message_topic = match message.get("topic") {
        Some(msg) => msg,
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

    if message_method.as_str() == Some("publish") {
        let _ = handle_publisher(message_topic.to_string(), socket, peer_addr, sender_map).await;
    } else if message_method.as_str() == Some("subscribe") {
        let _ = handle_subscriber(message_topic.to_string(), socket, peer_addr, sender_map).await;
    }

    Ok(())
}
