// Uncomment this block to pass the first stage

use core::str;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::{Buf, Bytes, BytesMut};
use cmd::{ping::Ping, Command};
use frame::{Frame, FrameCodec};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    fs::{read_to_string, File}, io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener
};
use tokio_util::codec::Framed;

mod cmd;
mod frame;
mod parse;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub type DB = Arc<Mutex<HashMap<String, (Bytes, Option<Instant>)>>>;

async fn parse_dbfile(dbfile: &str, db: DB) {
    let mut dbfile = File::open(dbfile).await.unwrap();
    let mut buf = Vec::new();
    dbfile.read_to_end(&mut buf).await.unwrap();
    let mut buf = BytesMut::from(&buf[..]);
    
    let header = &buf[..9];
    let header = str::from_utf8(header).unwrap();
    println!("header: {}", header);
    buf.advance(9);

    let mut tag = buf[0];
    assert_eq!(tag, 0xFA);
    buf.advance(1);
    while buf.has_remaining() {
        let name_len = buf[0] & 0b00111111;
        let name_len = name_len as usize;

        let name = &buf[1..1 + name_len];
        let name = str::from_utf8(name).unwrap();
        println!("name: {}", name);

        buf.advance(1 + name_len);

        let value_len = buf[0] & 0b00111111;
        let value_len = value_len as usize;

        let value = &buf[1..1 + value_len];
        let value = str::from_utf8(value).unwrap();
        println!("value: {}", value);

        buf.advance(1 + value_len);

        tag = buf[0];
        if tag == 0xFE {
            break;
        }
    }

}

#[tokio::test]
async fn test_parse_dbfile() {
    let db = Arc::new(Mutex::new(HashMap::new()));

    parse_dbfile("./dump.rdb", db.clone()).await;
}

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<String>>();

    let config = Arc::new(Mutex::new(HashMap::new()));
    if args.len() > 2 && (args[1] == "--dir" || args[3] == "--dbfilename") {
        config
            .lock()
            .unwrap()
            .insert("dir".to_string(), args[2].clone());
        config
            .lock()
            .unwrap()
            .insert("dbfilename".to_string(), args[4].clone());
    }
    let db = Arc::new(Mutex::new(HashMap::new()));

    parse_dbfile("./dump.rdb", db.clone()).await;

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");

                let db = db.clone();
                let config = config.clone();
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
                                let expires = set
                                    .expire
                                    .and_then(|expire| Instant::now().checked_add(expire));
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
                                            client.send(Frame::Null).await.unwrap();
                                            continue;
                                        }
                                    }
                                    client
                                        .send(Frame::Bulk(value.clone().into()))
                                        .await
                                        .unwrap();
                                } else {
                                    client.send(Frame::Null).await.unwrap();
                                }
                            }
                            Ok(Command::ConfigGet(cmd)) => {
                                let key = cmd.key;
                                let value = {
                                    let config = config.lock().unwrap();
                                    config.get(&key).cloned()
                                };
                                if value.is_some() {
                                    let frame = Frame::Array(vec![
                                        Frame::Bulk(key.into_bytes().into()),
                                        Frame::Bulk(value.unwrap().into_bytes().into()),
                                    ]);
                                    client.send(frame).await.unwrap();
                                } else {
                                    client.send(Frame::Null).await.unwrap();
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
