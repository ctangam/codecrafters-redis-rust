// Uncomment this block to pass the first stage

use core::str;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex}, time::Instant,
};

use bytes::{Buf, BytesMut};
use cmd::{ping::Ping, Command};
use frame::{Frame, FrameCodec};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::codec::Framed;

mod cmd;
mod frame;
mod parse;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");

                let db = db.clone();
                tokio::spawn(async move {
                    let mut client = Framed::new(stream, FrameCodec);
                    loop {
                        let frame = client.next().await.unwrap().unwrap();
                        println!("frame: {:?}", frame);

                        match Command::from(frame) {
                            Ok(Command::Ping(_)) => client
                                .send(Frame::Simple("PONG".to_string()))
                                .await
                                .unwrap(),
                            Ok(Command::Echo(echo)) => client
                                .send(Frame::Bulk(echo.message.into_bytes().into()))
                                .await
                                .unwrap(),
                            Ok(Command::Set(set)) => {
                                let expires = set.expire.and_then(|expire| {
                                    Instant::now().checked_add(expire)
                                });
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(set.key, (set.value, expires));
                                    drop(db);
                                }

                                client.send(Frame::Simple("OK".to_string())).await.unwrap();
                            }
                            Ok(Command::Get(get)) => {
                                let value = {
                                    let db = db.lock().unwrap();
                                    let value = db.get(&get.key).cloned();
                                    drop(db);
                                    value
                                };
                                if let Some((value, expires)) = value {
                                    if let Some(expires) = expires {
                                        let now = Instant::now();
                                        if now > expires {
                                            client.send(Frame::Simple("nil".to_string())).await.unwrap();
                                            continue;
                                        }
                                    }
                                    client
                                        .send(Frame::Bulk(value.clone().into()))
                                        .await
                                        .unwrap();
                                } else {
                                    client.send(Frame::Simple("nil".to_string())).await.unwrap();
                                }
                            }
                            Ok(Command::Unknown(_)) => {
                                continue;
                            }
                            Err(e) => {
                                println!("error: {}", e);
                                client.send(Frame::Error(e.to_string())).await.unwrap();
                            }
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
