// Uncomment this block to pass the first stage

use bytes::{Buf, BytesMut};
use frame::{Frame, FrameCodec};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::codec::Framed;

mod cmd;
mod frame;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    let mut client = Framed::new(stream, FrameCodec);
                    loop {
                        let frame = client.next().await.unwrap().unwrap();

                        match frame {
                            Frame::Simple(msg) => {
                                println!("simple: {}", msg);
                                if msg == "PING" {
                                    client.send(Frame::Simple("PONG".to_string())).await.unwrap();
                                }
                            }
                            Frame::Array(msg) => {
                                if msg.len() == 2 {
                                    if let Frame::Simple(cmd) = &msg[0] {
                                        if cmd == "ECHO" {
                                            if let Frame::Bulk(msg) = &msg[1] {
                                                client.send(Frame::Bulk(msg.clone())).await.unwrap();
                                            }
                                        }
                                    }
                                    
                                }
                            }
                            _ => {}
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
