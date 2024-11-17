// Uncomment this block to pass the first stage

use core::str;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    vec,
};

use bytes::{Buf, Bytes, BytesMut};
use clap::Parser;
use cmd::{Command, Executor};
use frame::{Frame, FrameCodec};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
};
use tokio_util::codec::Framed;

mod cmd;
mod frame;
mod parse;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

async fn parse_dbfile(mut buf: BytesMut, db: DB) {
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
                    let value = u64::from_le_bytes(buf[1..][..8].try_into().unwrap());
                    println!("{value:?} millis");
                    buf.advance(9);
                    let time = UNIX_EPOCH + Duration::from_millis(value);

                    let earlier = SystemTime::now();
                    if time < earlier {
                        Some(Instant::now())
                    } else {
                        Some(Instant::now() + time.duration_since(earlier).unwrap())
                    }
                } else if buf[0] == 0xFD {
                    let value = u32::from_le_bytes(buf[1..][..4].try_into().unwrap());
                    println!("{value:?} secs");
                    buf.advance(5);
                    let time = UNIX_EPOCH + Duration::from_secs(value as u64);

                    let earlier = SystemTime::now();
                    if time < earlier {
                        Some(Instant::now())
                    } else {
                        Some(Instant::now() + time.duration_since(earlier).unwrap())
                    }
                } else {
                    None
                };
                let value_type = buf[0];
                assert_eq!(value_type, 0);
                buf.advance(1);
                let key = string_decode(&mut buf);
                let value = string_decode(&mut buf);
                db.insert(key, (value.into(), expire));
            }
        }
    }

    println!("db: {:?}", db);

    assert_eq!(buf[0], 0xFF);
    println!("end of file");
    buf.advance(1);

    let _crc = &buf[..8];
    buf.advance(8);
}

fn _list_decode(src: &mut BytesMut) -> Vec<String> {
    let size = size_decode(src);
    (0..size).map(|_| string_decode(src)).collect()
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

#[test]
fn test_expire() {
    let value = u32::from_le_bytes([0x52, 0xED, 0x2A, 0x66]);
    let time = UNIX_EPOCH + Duration::from_secs(value as u64);

    let expire = Some(Instant::now() + time.elapsed().unwrap());

    println!("{expire:?}");
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

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    dir: Option<PathBuf>,
    #[arg(long)]
    dbfilename: Option<String>,

    #[arg(long, default_value_t = 6379)]
    port: u32,
    #[arg(long)]
    replicaof: Option<String>,
}

pub type DB = Arc<Mutex<HashMap<String, (Bytes, Option<Instant>)>>>;
pub type STREAMS = Arc<Mutex<HashMap<String, Vec<((u128, u64), Vec<(String, Bytes)>)>>>>;

#[derive(Clone)]
pub struct Env {
    pub db: DB,
    pub config: Arc<Mutex<HashMap<String, String>>>,
    pub streams: STREAMS,
    pub tx: broadcast::Sender<(Frame, Option<mpsc::Sender<u64>>)>,
    pub streams_tx: broadcast::Sender<(String, Option<Vec<((u128, u64), Vec<(String, Bytes)>)>>)>,
}

impl Env {
    pub fn new() -> Self {
        let db: DB = Arc::new(Mutex::new(HashMap::new()));
        let config = Arc::new(Mutex::new(HashMap::new()));
        let streams: STREAMS = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _) = broadcast::channel(32);
        let (streams_tx, _) = broadcast::channel(32);
        Self {
            db,
            config,
            streams,
            tx,
            streams_tx,
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut role: &'static str = "master";
    let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    let master_repl_offset = 0;

    let env = Env::new();

    if let Some(dir) = args.dir.as_deref() {
        env.config
            .lock()
            .unwrap()
            .insert("dir".to_string(), dir.to_string_lossy().to_string());
        if let Some(dbfilename) = args.dbfilename.as_deref() {
            env.config
                .lock()
                .unwrap()
                .insert("dbfilename".to_string(), dbfilename.to_string());

            let path = dir.join(dbfilename);
            if path.exists() {
                let mut dbfile = File::open(path).await.unwrap();
                let mut buf = Vec::new();
                dbfile.read_to_end(&mut buf).await.unwrap();
                let buf = BytesMut::from(&buf[..]);
                parse_dbfile(buf, env.db.clone()).await;
            }
        }
    }

    let address = format!("127.0.0.1:{}", args.port);

    let master = if let Some(replicaof) = args.replicaof.as_deref() {
        role = "slave";
        replicaof.split_once(" ")
    } else {
        None
    };

    if let Some((host, port)) = master {
        let address = format!("{}:{}", host, port);
        let mut master = Framed::new(TcpStream::connect(address).await.unwrap(), FrameCodec);
        master
            .send(Frame::Array(vec![Frame::Bulk(
                "PING".to_string().into_bytes().into(),
            )]))
            .await
            .unwrap();

        let (_, reply) = master.next().await.unwrap().unwrap();
        assert_eq!(reply, Frame::Simple("PONG".to_string()));

        master
            .send(Frame::Array(vec![
                Frame::Bulk("REPLCONF".to_string().into_bytes().into()),
                Frame::Bulk("listening-port".to_string().into_bytes().into()),
                Frame::Bulk(args.port.to_string().into_bytes().into()),
            ]))
            .await
            .unwrap();

        let (_, reply) = master.next().await.unwrap().unwrap();
        assert_eq!(reply, Frame::Simple("OK".to_string()));

        master
            .send(Frame::Array(vec![
                Frame::Bulk("REPLCONF".to_string().into_bytes().into()),
                Frame::Bulk("capa".to_string().into_bytes().into()),
                Frame::Bulk("psync2".to_string().into_bytes().into()),
            ]))
            .await
            .unwrap();

        let (_, reply) = master.next().await.unwrap().unwrap();
        assert_eq!(reply, Frame::Simple("OK".to_string()));

        master
            .send(Frame::Array(vec![
                Frame::Bulk("PSYNC".to_string().into_bytes().into()),
                Frame::Bulk("?".to_string().into_bytes().into()),
                Frame::Bulk("-1".to_string().into_bytes().into()),
            ]))
            .await
            .unwrap();

        let (_, reply) = master.next().await.unwrap().unwrap();
        let _replid = if let Frame::Simple(s) = &reply {
            s.split_once(" ").unwrap().1
        } else {
            panic!()
        };

        let (_, rdbfile) = master.next().await.unwrap().unwrap();
        if let Frame::File(content) = rdbfile {
            parse_dbfile(BytesMut::from(content), env.db.clone()).await;
        }

        let db = env.db.clone();
        tokio::spawn(async move {
            let mut offset = 0usize;
            loop {
                let (n, frame) = master.next().await.unwrap().unwrap();
                match Command::from(frame) {
                    Ok(Command::Ping(_)) => offset += n,
                    Ok(Command::Set(set)) => {
                        let expires = set
                            .expire
                            .and_then(|expire| Instant::now().checked_add(expire));
                        {
                            let mut db = db.lock().unwrap();
                            db.insert(set.key, (set.value, expires));
                            drop(db);
                        }
                        offset += n;
                    }
                    Ok(Command::Replconf(replconf)) => {
                        if let Some(_ack) = replconf.ack {
                            master
                                .send(Frame::Array(vec![
                                    Frame::Bulk("REPLCONF".to_string().into()),
                                    Frame::Bulk("ACK".to_string().into()),
                                    Frame::Bulk(offset.to_string().into()),
                                ]))
                                .await
                                .unwrap();
                            offset += n;
                        }
                    }
                    Err(e) => {
                        println!("error: {}", e);
                        master.send(Frame::Error(e.to_string())).await.unwrap();
                    }
                    _ => unimplemented!(),
                }
            }
        });
    }

    let listener = TcpListener::bind(address).await.unwrap();
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");

                let db = env.db.clone();
                let streams = env.streams.clone();
                let config = env.config.clone();
                let tx = env.tx.clone();
                let streams_tx = env.streams_tx.clone();
                let env = env.clone();

                tokio::spawn(async move {
                    let mut client = Framed::new(stream, FrameCodec);
                    let mut received = false;
                    let mut trans = false;
                    let mut queue: Vec<Command> = Vec::new();
                    loop {
                        if let Some(Ok((_, frame))) = client.next().await {
                            match Command::from(frame.clone()) {
                                Ok(Command::Ping(_)) => client
                                    .send(Frame::Simple("PONG".to_string()))
                                    .await
                                    .unwrap(),
                                Ok(Command::Echo(echo)) => client
                                    .send(Frame::Bulk(echo.message.into_bytes().into()))
                                    .await
                                    .unwrap(),
                                Ok(Command::Rtype(rtype)) => {
                                    if db.lock().unwrap().contains_key(&rtype.key) {
                                        client
                                            .send(Frame::Simple("string".to_string()))
                                            .await
                                            .unwrap();
                                    } else if streams.lock().unwrap().contains_key(&rtype.key) {
                                        client
                                            .send(Frame::Simple("stream".to_string()))
                                            .await
                                            .unwrap();
                                    } else {
                                        client
                                            .send(Frame::Simple("none".to_string()))
                                            .await
                                            .unwrap();
                                    }
                                }
                                Ok(Command::Xadd(xadd)) => {
                                    let stream =
                                        streams.lock().unwrap().get(&xadd.stream_key).cloned();
                                    let last_id =
                                        stream.and_then(|s| s.last().and_then(|e| Some(e.0)));
                                    match parse_id(&xadd.id, last_id) {
                                        Ok(new_id) => {
                                            streams
                                                .lock()
                                                .unwrap()
                                                .entry(xadd.stream_key.clone())
                                                .and_modify(|pairs| {
                                                    pairs.push((new_id, xadd.pairs.clone()))
                                                })
                                                .or_insert(vec![(new_id, xadd.pairs.clone())]);

                                            client
                                                .send(Frame::Bulk(
                                                    format!("{}-{}", new_id.0, new_id.1).into(),
                                                ))
                                                .await
                                                .unwrap();

                                            let _ = streams_tx.send((
                                                xadd.stream_key,
                                                Some(vec![(new_id, xadd.pairs)]),
                                            ));
                                        }
                                        Err(e) => {
                                            client.send(Frame::Error(e.to_string())).await.unwrap()
                                        }
                                    }
                                }
                                Ok(Command::Xrange(xrange)) => {
                                    let start = if &xrange.start != "-" {
                                        parse_id(&xrange.start, None).ok()
                                    } else {
                                        None
                                    };
                                    let end = if &xrange.end != "+" {
                                        parse_id(&xrange.end, None).ok()
                                    } else {
                                        None
                                    };
                                    let entries = streams
                                        .lock()
                                        .unwrap()
                                        .get(&xrange.stream_key)
                                        .and_then(|s| {
                                            let start_index = if let Some(start) = start {
                                                s.binary_search_by(|e| {
                                                    e.0 .0.cmp(&start.0).then(e.0 .1.cmp(&start.1))
                                                })
                                                .unwrap()
                                            } else {
                                                0
                                            };
                                            let end_index = if let Some(end) = end {
                                                s.binary_search_by(|e| {
                                                    e.0 .0.cmp(&end.0).then(e.0 .1.cmp(&end.1))
                                                })
                                                .unwrap()
                                            } else {
                                                s.len() - 1
                                            };
                                            let entries = s[start_index..=end_index].to_vec();
                                            Some(entries)
                                        });
                                    let frame = if let Some(entries) = entries {
                                        let entries = entries
                                            .into_iter()
                                            .map(|entry| {
                                                let pairs = entry
                                                    .1
                                                    .into_iter()
                                                    .flat_map(|pair| {
                                                        vec![
                                                            Frame::Bulk(pair.0.into()),
                                                            Frame::Bulk(pair.1.into()),
                                                        ]
                                                        .into_iter()
                                                    })
                                                    .collect();
                                                Frame::Array(vec![
                                                    Frame::Bulk(
                                                        format!("{}-{}", entry.0 .0, entry.0 .1)
                                                            .into(),
                                                    ),
                                                    Frame::Array(pairs),
                                                ])
                                            })
                                            .collect();
                                        Frame::Array(entries)
                                    } else {
                                        Frame::Null
                                    };
                                    client.send(frame).await.unwrap();
                                }
                                Ok(Command::Xread(xread)) => {
                                    let mut streams_rx = streams_tx.subscribe();
                                    let mut results = Vec::new();
                                    while results.is_empty() {
                                        results = xread
                                            .streams
                                            .iter()
                                            .map(|(stream_key, id)| {
                                                let start = parse_id(&id, None).ok();
                                                let entries = streams
                                                    .lock()
                                                    .unwrap()
                                                    .get(stream_key)
                                                    .and_then(|s| {
                                                        let start_index = if let Some(start) = start
                                                        {
                                                            match s.binary_search_by(|e| {
                                                                e.0 .0
                                                                    .cmp(&start.0)
                                                                    .then(e.0 .1.cmp(&start.1))
                                                            }) {
                                                                Ok(i) => i + 1,
                                                                Err(i) => i,
                                                            }
                                                        } else {
                                                            0
                                                        };

                                                        let entries = s[start_index..].to_vec();
                                                        if entries.is_empty() {
                                                            None
                                                        } else {
                                                            Some(entries)
                                                        }
                                                    });
                                                (stream_key.clone(), entries)
                                            })
                                            .collect();

                                        if let Some(block_millis) = xread.block_millis {
                                            if block_millis != 0 {
                                                tokio::select! {
                                                    _ = tokio::time::sleep(Duration::from_millis(
                                                        block_millis,
                                                    )) => {},
                                                    Ok(stream) = streams_rx.recv() => {
                                                        if xread.streams.iter().find(|(key, _)| *key == stream.0).is_some() {
                                                            results = vec![stream];
                                                        }
                                                    }
                                                }
                                            } else {
                                                loop {
                                                    let stream = streams_rx.recv().await.unwrap();
                                                    if xread
                                                        .streams
                                                        .iter()
                                                        .find(|(key, _)| *key == stream.0)
                                                        .is_some()
                                                    {
                                                        results = vec![stream];
                                                        break;
                                                    }
                                                }
                                            }
                                        };
                                    }
                                    let streams: Vec<_> = results
                                        .into_iter()
                                        .filter_map(|(stream_key, entries)| {
                                            if let Some(entries) = entries {
                                                let entries = entries
                                                    .into_iter()
                                                    .map(|entry| {
                                                        let pairs = entry
                                                            .1
                                                            .into_iter()
                                                            .flat_map(|pair| {
                                                                vec![
                                                                    Frame::Bulk(pair.0.into()),
                                                                    Frame::Bulk(pair.1.into()),
                                                                ]
                                                                .into_iter()
                                                            })
                                                            .collect();
                                                        Frame::Array(vec![
                                                            Frame::Bulk(
                                                                format!(
                                                                    "{}-{}",
                                                                    entry.0 .0, entry.0 .1
                                                                )
                                                                .into(),
                                                            ),
                                                            Frame::Array(pairs),
                                                        ])
                                                    })
                                                    .collect();
                                                Some(Frame::Array(vec![
                                                    Frame::Bulk(stream_key.into()),
                                                    Frame::Array(entries),
                                                ]))
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();

                                    let frame = if streams.is_empty() {
                                        Frame::Null
                                    } else {
                                        Frame::Array(streams)
                                    };
                                    client.send(frame).await.unwrap();
                                }
                                Ok(Command::Incr(incr)) => {
                                    if trans {
                                        queue.push(Command::Incr(incr));
                                        client.send(Frame::Simple("QUEUED".into())).await.unwrap();
                                    } else {
                                        let frame = incr.exec(env.clone()).await;
                                        client.send(frame).await.unwrap();
                                    }
                                }
                                Ok(Command::Multi(_)) => {
                                    trans = true;
                                    client.send(Frame::Simple("OK".into())).await.unwrap();
                                }
                                Ok(Command::Exec(_)) => {
                                    if trans {
                                        let mut frames = Vec::with_capacity(queue.len());

                                        for cmd in &queue {
                                            let frame = cmd.exec(env.clone()).await;
                                            frames.push(frame);
                                        }

                                        client.send(Frame::Array(frames)).await.unwrap();
                                        trans = false;
                                    } else {
                                        client
                                            .send(Frame::Error("ERR EXEC without MULTI".into()))
                                            .await
                                            .unwrap();
                                    }
                                }
                                Ok(Command::Discard(_)) => {
                                    if trans {
                                        client.send(Frame::Simple("OK".into())).await.unwrap();
                                        trans = false;
                                    } else {
                                        client
                                            .send(Frame::Error("ERR DISCARD without MULTI".into()))
                                            .await
                                            .unwrap();
                                    }
                                }
                                Ok(Command::Set(set)) => {
                                    received = true;
                                    let _ = env.tx.send((frame, None));
                                    if trans {
                                        queue.push(Command::Set(set));
                                        client.send(Frame::Simple("QUEUED".into())).await.unwrap();
                                    } else {
                                        let frame = set.exec(env.clone()).await;
                                        client.send(frame).await.unwrap();
                                    }
                                }
                                Ok(Command::Get(get)) => {
                                    if trans {
                                        queue.push(Command::Get(get));
                                        client.send(Frame::Simple("QUEUED".into())).await.unwrap();
                                    } else {
                                        let frame = get.exec(env.clone()).await;
                                        client.send(frame).await.unwrap();
                                    }
                                }
                                Ok(Command::ConfigGet(cmd)) => {
                                    let key = cmd.key;
                                    let value = config.lock().unwrap().get(&key).cloned();

                                    if value.is_some() {
                                        let frame = Frame::Array(vec![
                                            Frame::Bulk(key.into()),
                                            Frame::Bulk(value.unwrap().into()),
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
                                        db.keys()
                                            .into_iter()
                                            .filter(|key| {
                                                key.starts_with(left) && key.ends_with(right)
                                            })
                                            .map(|key| Frame::Bulk(key.clone().into()))
                                            .collect::<Vec<_>>()
                                    };
                                    client.send(Frame::Array(keys)).await.unwrap();
                                }
                                Ok(Command::Info(info)) => {
                                    if info.replication {
                                        client
                                        .send(Frame::Bulk(
                                            format!("role:{role}\r\nmaster_repl_offset:{master_repl_offset}\r\nmaster_replid:{master_replid}").into_bytes().into(),
                                        ))
                                        .await
                                        .unwrap();
                                    }
                                }
                                Ok(Command::Wait(_)) => {
                                    println!("receiver: {}", tx.receiver_count());
                                    if !received {
                                        client
                                            .send(Frame::Integer(tx.receiver_count() as u64))
                                            .await
                                            .unwrap();
                                        continue;
                                    }
                                    let (resp_tx, mut resp_rx) = mpsc::channel::<u64>(32);
                                    tx.send((frame, Some(resp_tx))).unwrap();

                                    let mut acknowledged = 0;
                                    while let Some(n) = resp_rx.recv().await {
                                        acknowledged += n;
                                    }
                                    println!("num of replicas acknowledged: {acknowledged}");
                                    client.send(Frame::Integer(acknowledged)).await.unwrap();
                                }
                                Ok(Command::Unknown(_)) => {
                                    continue;
                                }
                                Err(e) => {
                                    println!("error: {}", e);
                                    client.send(Frame::Error(e.to_string())).await.unwrap();
                                }

                                Ok(Command::Replconf(replconf)) => {
                                    let port = replconf.port.unwrap();
                                    let mut replica = client;
                                    break tokio::spawn(async move {
                                        replica
                                            .send(Frame::Simple("OK".to_string()))
                                            .await
                                            .unwrap();
                                        let (_, frame) = replica.next().await.unwrap().unwrap();
                                        if let Ok(Command::Replconf(_replconf)) =
                                            Command::from(frame)
                                        {
                                            replica
                                                .send(Frame::Simple("OK".to_string()))
                                                .await
                                                .unwrap();
                                        }
                                        let (_, frame) = replica.next().await.unwrap().unwrap();
                                        if let Ok(Command::Psync(_)) = Command::from(frame) {
                                            replica
                                                .send(Frame::Simple(format!(
                                                    "FULLRESYNC {repl_id} {repl_offset}",
                                                    repl_id =
                                                        "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                                                    repl_offset = "0",
                                                )))
                                                .await
                                                .unwrap();
                                            let content = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                                            let content = hex::decode(content).unwrap();
                                            replica
                                                .send(Frame::File(Bytes::from(content)))
                                                .await
                                                .unwrap();
                                        }
                                        //handshake done
                                        let mut rx = tx.subscribe();
                                        loop {
                                            let (frame, resp_tx) = rx.recv().await.unwrap();
                                            match Command::from(frame.clone()) {
                                                Ok(Command::Set(_set)) => {
                                                    replica.send(frame).await.unwrap();
                                                }
                                                Ok(Command::Wait(wait)) => {
                                                    replica
                                                        .send(Frame::Array(vec![
                                                            Frame::Bulk(
                                                                "REPLCONF".to_string().into(),
                                                            ),
                                                            Frame::Bulk(
                                                                "GETACK".to_string().into(),
                                                            ),
                                                            Frame::Bulk("*".to_string().into()),
                                                        ]))
                                                        .await
                                                        .unwrap();
                                                    let acknowledge: u64 = tokio::select! {
                                                        _ = tokio::time::sleep(Duration::from_millis(wait.timeout)) => 0,
                                                        _ = replica.next() => 1,
                                                    };
                                                    println!("replica {port} {acknowledge}");
                                                    resp_tx
                                                        .unwrap()
                                                        .send(acknowledge)
                                                        .await
                                                        .unwrap();
                                                }
                                                _ => unimplemented!(),
                                            }
                                        }
                                    });
                                }

                                _ => unreachable!(),
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

fn parse_id(new_id: &str, last_id: Option<(u128, u64)>) -> Result<(u128, u64)> {
    match new_id.split_once("-") {
        None => {
            let millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let mut seq = 0;
            if let Some((last_millis, last_seq)) = last_id {
                if millis == last_millis {
                    seq = last_seq + 1;
                }
            }
            Ok((millis, seq))
        }
        Some((millis, "*")) | Some((millis, "")) => {
            let millis = u128::from_str_radix(millis, 10).unwrap();
            let mut seq = if millis == 0 { 1 } else { 0 };
            if let Some((last_millis, last_seq)) = last_id {
                if millis == last_millis {
                    seq = last_seq + 1;
                }
            }
            Ok((millis, seq))
        }
        Some((millis, seq)) => {
            let millis = u128::from_str_radix(millis, 10).unwrap();
            let seq = u64::from_str_radix(seq, 10).unwrap();
            if millis == 0 && seq == 0 {
                return Err("ERR The ID specified in XADD must be greater than 0-0".into());
            }
            if let Some((last_millis, last_seq)) = last_id {
                if millis < last_millis || (millis == last_millis && seq <= last_seq) {
                    return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".into());
                }
            }
            Ok((millis, seq))
        }
    }
}

#[test]
fn test_glob() {
    let p = "*".to_string();
    let (left, right) = p.split_once("*").unwrap();
    let buf = vec!["foo", "baz"];
    let result = buf
        .into_iter()
        .filter(|key| key.starts_with(left) && key.ends_with(right))
        .collect::<Vec<_>>();
    assert_eq!(result, vec!["foo", "baz"]);
}
