// Uncomment this block to pass the first stage

use core::str;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::{Buf, Bytes, BytesMut};
use cmd::{ping::Ping, Command};
use frame::{Frame, FrameCodec};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    fs::{read_to_string, File},
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::codec::Framed;

mod cmd;
mod frame;
mod parse;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub type DB = Arc<Mutex<HashMap<String, (Bytes, Option<Instant>)>>>;

async fn parse_dbfile<T: AsRef<Path>>(dbfile: T, db: DB) {
    let mut dbfile = File::open(dbfile).await.unwrap();
    let mut buf = Vec::new();
    dbfile.read_to_end(&mut buf).await.unwrap();
    let mut buf = BytesMut::from(&buf[..]);

    println!("{:?}", String::from_utf8_lossy(&buf[..]));
    let header = &buf[..9];
    let header = str::from_utf8(header).unwrap();
    println!("header: {}", header);
    buf.advance(9);

    let mut metadatas = Vec::new();
    while buf[0] == 0xFA {
        buf.advance(1);

        let name = string_decode(&mut buf);
        let value = string_decode(&mut buf);

        metadatas.push((name, value));
    }
    println!("metadatas: {:?}", metadatas);

    let mut db = db.lock().unwrap();
    while buf[0] == 0xFE {
        buf.advance(1);

        let index = size_decode(&mut buf);
        println!("index: {}", index);
        if buf[0] == 0xFB {
            buf.advance(1);
            let size = size_decode(&mut buf);
            println!("size: {}", size);
            let expire_size = size_decode(&mut buf);
            println!("expire_size: {}", expire_size);

            for _ in 0..size {
                let expire = if buf[0] == 0xFC {
                    buf.advance(1);
                    let value = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    Some(Instant::now() + Duration::from_millis(value))
                } else if buf[0] == 0xFD {
                    buf.advance(1);
                    let value = u64::from_le_bytes(buf[..4].try_into().unwrap());
                    Some(Instant::now() + Duration::from_secs(value))
                } else {
                    None
                };
                println!("expire: {:?}", expire);
                let value_type = buf[0];
                assert_eq!(value_type, 0);
                buf.advance(1);
                let key = string_decode(&mut buf);
                println!("{key:?}");
                let value = string_decode(&mut buf);
                println!("{value:?}");
                db.insert(key, (value.into(), expire));
            }
        }
    }

    println!("db: {:?}", db);

    assert_eq!(buf[0], 0xFF);
    println!("end of file");
    buf.advance(1);

    let crc = &buf[..8];
    buf.advance(8);
}

fn list_decode(src: &mut BytesMut) -> Vec<String> {
    let size = size_decode(src);
    (0..size)
        .map(|_| string_decode(src))
        .collect()
}

fn string_decode(src: &mut BytesMut) -> String {
    match src[0] {
        0xC0 => {
            let s = src[1].to_string();
            src.advance(2);
            s
        }

        0xC1 => {
            let s = u16::from_le_bytes([src[1], src[2]]).to_string();
            src.advance(3);
            s
        }

        0xC2 => {
            let s = u32::from_le_bytes([src[1], src[2], src[3], src[4]]).to_string();
            src.advance(5);
            s
        }

        _ => {
            let len = size_decode(src);
            let s = String::from_utf8(src[..len].to_vec()).unwrap();
            src.advance(len);
            s
        }
    }
}

fn size_decode(src: &mut BytesMut) -> usize {
    let indicator = (src[0] & 0b1100_0000) >> 6;
    let b0 = src[0] & 0b0011_1111;
    match indicator {
        0b00 => {
            let size = b0 as usize;
            src.advance(1);
            size
        }

        0b01 => {
            let size = (b0 as usize) << 8 | (src[1] as usize);
            src.advance(2);
            size
        }

        0b10 => {
            let size = (b0 as usize) << 32
                | (src[1] as usize) << 24
                | (src[2] as usize) << 16
                | (src[3] as usize) << 8
                | (src[4] as usize);

            src.advance(4);
            size
        }

        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_parse_dbfile() {
    let db = Arc::new(Mutex::new(HashMap::new()));

    parse_dbfile("./dump.rdb", db.clone()).await;
}

#[test]
fn test_string_decode() {
    let encoded = vec![
        0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
    ];
    let mut buf = BytesMut::from(&encoded[..]);
    let s = string_decode(&mut buf);
    assert_eq!(s, "Hello, World!");


    let encoded = vec![0xC0, 0x7B];
    let mut buf = BytesMut::from(&encoded[..]);
    let s = string_decode(&mut buf);
    assert_eq!(s, "123");

    let encoded = vec![0xC1, 0x39, 0x30];
    let mut buf = BytesMut::from(&encoded[..]);
    let s = string_decode(&mut buf);
    assert_eq!(s, "12345");

    let encoded = vec![0xC2, 0x87, 0xD6, 0x12, 0x00];
    let mut buf = BytesMut::from(&encoded[..]);
    let s = string_decode(&mut buf);
    assert_eq!(s, "1234567");
}

#[test]
fn test_size_decode() {
    let mut buf = BytesMut::from(&0x0A_u8.to_be_bytes()[..]);
    let s = size_decode(&mut buf);
    assert_eq!(s, 10);

    let mut buf = BytesMut::from(&0x42BC_u16.to_be_bytes()[..]);
    let s = size_decode(&mut buf);
    assert_eq!(s, 700);

    let mut buf = BytesMut::from(&(0x8000004268_u64 << 24).to_be_bytes()[..]);
    let s = size_decode(&mut buf);
    assert_eq!(s, 17000);
}

#[tokio::main]
async fn main() {
    let db = Arc::new(Mutex::new(HashMap::new()));

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

        let path = PathBuf::from(&args[2]).join(&args[4]).to_path_buf();
        if path.exists() {
            parse_dbfile(path, db.clone()).await;
        }
    }

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
                            Ok(Command::Keys(keys)) => {
                                let (left, right) = keys.pattern.split_once("*").unwrap();
                                let keys = {
                                    let db = db.lock().unwrap();
                                    db.keys().into_iter().filter(|key| {
                                        key.starts_with(left) && key.ends_with(right)
                                    }).map(|key| Frame::Bulk(key.clone().into_bytes().into())).collect::<Vec<_>>()
                                };
                                client.send(Frame::Array(keys)).await.unwrap();
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

#[test]
fn test_glob() {
    let p = "*".to_string();
    let (left, right) = p.split_once("*").unwrap();
    let buf = vec!["foo", "baz"];
    let result = buf.into_iter().filter(|key| key.starts_with(left) && key.ends_with(right) ).collect::<Vec<_>>();
    assert_eq!(result, vec!["foo", "baz"]);
}
