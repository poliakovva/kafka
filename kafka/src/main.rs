//!Simplified Kafka is a service that provides an interface for messaging
///between different applications on different devices.
///The main abstraction in kafka is a topic, which is essentially a channel
//that can be accessed either to send messages to it or to listen to and receive messages from it.
use log::info;
use std::{collections::HashMap, net::IpAddr, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{broadcast::Sender, Mutex},
};
extern crate clap;

use clap::Parser;
pub mod handle_client;
pub mod publisher;
pub mod subscriber;
use handle_client::handle_client;

#[derive(Parser, Debug)]
#[command(
    author = "Vladyslav Poliakov",
    version = "0.1.0",
    about = "Simplified Kafka is a service that provides an interface for 
    messaging between different applications on different devices.",
    long_about = "Simplified Kafka is a service that provides an interface for messaging
     between different applications on different devices.

    The main abstraction in kafka is a topic, which is essentially a channel 
    that can be accessed either to send messages to it or to listen to and receive messages from it."
)]
pub struct Args {
    #[arg(short, long, help = "ip address for server start up")]
    pub address: IpAddr,
    #[arg(short, long, help = "port for server start up")]
    pub port: u16,
}

async fn run(ip: IpAddr, port: u16) {
    let listener = match TcpListener::bind((ip, port)).await {
        Ok(tcp) => {
            println!("Start kafka server on address {}, {}", ip, port);
            tcp
        }
        Err(_) => panic!(
            "Not able to start server on {}:{} -- port is busy",
            ip, port
        ),
    };

    let sender_map: Arc<Mutex<HashMap<String, Sender<Vec<u8>>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let sender_map_copy = sender_map.clone();
                tokio::spawn(handle_client(socket, addr, sender_map_copy));
            }
            Err(_) => info!("failed connection"),
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let ip = args.address;
    let port = args.port;
    run(ip, port).await;
}
