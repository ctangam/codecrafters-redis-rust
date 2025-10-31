// Uncomment this block to pass the first stage

use core::{panic::PanicMessage, str};
use std::{
    cmp::min,
    collections::{HashMap, HashSet},
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
    select,
    sync::{broadcast, mpsc, oneshot},
};
use tokio_stream::StreamMap;
use tokio_util::codec::Framed;

use crate::{
    cmd::{
        blpop::Blpop, geoadd::Geoadd, geodist::Geodist, geopos::Geopos, geosearch::Geosearch,
        llen::Llen, lpop::Lpop, lpush::Lpush, lrange::Lrange, publish::Publish, rpush::Rpush,
        zadd::Zadd, zcard::Zcard, zrange::Zrange, zrank::Zrank, zrem::Zrem, zscore::Zscore,
    },
    dbfile::parse_dbfile, geo::{cal_distance, cal_loc_score, decode_coordinates, search_within_radius, MAX_LATITUDE, MAX_LONGITUDE, MIN_LATITUDE, MIN_LONGITUDE},
};

mod cmd;
mod dbfile;
mod env;
mod frame;
mod parse;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

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

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut role: &'static str = "master";
    let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    let master_repl_offset = 0;

    let env = env::Env::new();

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
                                        .map(|s| {
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
                                            
                                            s[start_index..=end_index].to_vec()
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
                                                            Frame::Bulk(pair.1),
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
                                                let start = parse_id(id, None).ok();
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
                                                        if xread.streams.iter().any(|(key, _)| *key == stream.0) {
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
                                                        .any(|(key, _)| *key == stream.0)
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
                                                                    Frame::Bulk(pair.1),
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
                                        Frame::NullArray
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
                                            .send(Frame::Integer(tx.receiver_count() as i64))
                                            .await
                                            .unwrap();
                                        continue;
                                    }
                                    let (resp_tx, mut resp_rx) = mpsc::channel::<i64>(32);
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
                                    println!("error: {e}");
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
                                                    let acknowledge = tokio::select! {
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
                                },
                                Ok(Command::Rpush(rpush)) => {
                                    dbg!(&rpush);
                                    let Rpush{list_key, elements} = rpush;
                                    let size = {
                                        let mut lists = env.lists.lock().unwrap();
                                        let list = lists.entry(list_key.clone()).or_default();
                                        list.extend(elements);
                                        list.len()
                                    };
                                    if let Some(wait_list) = env.wait_lists.lock().unwrap().get_mut(&list_key) {
                                        if !wait_list.is_empty() {
                                            let element = env.lists.lock().unwrap().get_mut(&list_key).map(|list| list.remove(0));
                                            wait_list.remove(0).send(element.unwrap()).ok();
                                        }
                                    }
                                    client.send(Frame::Integer(size as i64)).await.unwrap();
                                },
                                Ok(Command::Lrange(lrange)) => {
                                    dbg!(&lrange);
                                    let Lrange{list_key, start, stop} = lrange;
                                    
                                    let elements = {
                                        let lists = env.lists.lock().unwrap();
                                        if let Some(list) = lists.get(&list_key) {
                                            let start = if (start + list.len() as i64) < 0 {
                                                0
                                            } else if start < 0 {
                                                start + list.len() as i64
                                            } else {
                                                start
                                            } as usize;
                                            let stop = if (stop + list.len() as i64) < 0 {
                                                0
                                            } else if stop < 0 {
                                                stop + list.len() as i64
                                            } else {
                                                stop
                                            } as usize;
                                            let stop = min(stop, list.len() - 1);
                                            if start > stop || start >= list.len() {
                                                vec![]
                                            } else {
                                                list[start..=stop].iter().map(|s| Frame::Bulk(s.clone().into())).collect()
                                            }
                                        } else {
                                            vec![]
                                        }
                                    };
                                    client.send(Frame::Array(elements)).await.unwrap();
                                },
                                Ok(Command::Lpush(lpush)) => {
                                    dbg!(&lpush);
                                    let Lpush{list_key, elements} = lpush;
                                    let size = {
                                        let mut lists = env.lists.lock().unwrap();
                                        let list = lists.entry(list_key.clone()).or_default();
                                        elements.iter().for_each(|e| list.insert(0, e.clone()));
                                        list.len()
                                    };
                                    if let Some(wait_list) = env.wait_lists.lock().unwrap().get_mut(&list_key) {
                                        if !wait_list.is_empty() {
                                            let element = env.lists.lock().unwrap().get_mut(&list_key).map(|list| list.remove(0));
                                            wait_list.remove(0).send(element.unwrap()).ok();
                                        }
                                    }
                                    client.send(Frame::Integer(size as i64)).await.unwrap();
                                },
                                Ok(Command::Llen(llen)) => {
                                    dbg!(&llen);
                                    let Llen{list_key} = llen;
                                    let size = {
                                        let lists = env.lists.lock().unwrap();
                                        if let Some(list) = lists.get(&list_key) {
                                            list.len()
                                        } else {
                                            0
                                        }
                                    };
                                    client.send(Frame::Integer(size as i64)).await.unwrap();
                                },
                                Ok(Command::Lpop(lpop)) => {
                                    dbg!(&lpop);
                                    let Lpop{list_key, count} = lpop;
                                    if let Some(count) = count {
                                        let elements = {
                                            let mut lists = env.lists.lock().unwrap();
                                            if let Some(list) = lists.get_mut(&list_key) {
                                                let count = min(count, list.len());
                                                list.drain(0..count).map(|s| Frame::Bulk(s.into())).collect()
                                            } else {
                                                vec![]
                                            }
                                        };
                                        client.send(Frame::Array(elements)).await.unwrap();
                                    } else {
                                        let element = {
                                            let mut lists = env.lists.lock().unwrap();
                                            if let Some(list) = lists.get_mut(&list_key) {
                                                if !list.is_empty() {
                                                    Some(Frame::Bulk(list.remove(0).into()))
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            }
                                        };
                                        if let Some(element) = element {
                                            client.send(element).await.unwrap();
                                        } else {
                                            client.send(Frame::Null).await.unwrap();
                                        }
                                    }
                                    
                                },
                                Ok(Command::Blpop(blpop)) => {
                                    dbg!(&blpop);
                                    let Blpop{list_key, timeout} = blpop;
                                    let mut elements = vec![];
                                    // Scope the MutexGuard so it is dropped before any await
                                    let mut found = false;
                                    {
                                        let mut lists = env.lists.lock().unwrap();
                                        if let Some(list) = lists.get_mut(&list_key) {
                                            if !list.is_empty() {
                                                let element = list.remove(0);
                                                elements = vec![Frame::Bulk(list_key.clone().into()), Frame::Bulk(element.into())];
                                                found = true;
                                            }
                                        }
                                    }
                                    if !found {
                                        let (tx, rx) = oneshot::channel();
                                        {
                                            let mut wait_lists = env.wait_lists.lock().unwrap();
                                            wait_lists.entry(list_key.clone()).or_default().push(tx);
                                        }
                                        if timeout != 0 {
                                            tokio::select! {
                                                _ = tokio::time::sleep(Duration::from_secs(timeout)) => {},
                                                Ok(element) = rx => {
                                                    elements = vec![Frame::Bulk(list_key.clone().into()), Frame::Bulk(element.into())];
                                                }
                                            }
                                        } else {
                                            let element = rx.await.unwrap();
                                            elements = vec![Frame::Bulk(list_key.clone().into()), Frame::Bulk(element.into())];
                                        }
                                    }
                                    client.send(if elements.is_empty() { Frame::Null } else { Frame::Array(elements) }).await.unwrap();
                                },
                                Ok(Command::Subscribe(subscribe)) => {
                                    dbg!(&subscribe);
                                    let mut stream_map = StreamMap::new();
                                    let mut channels = Vec::new();

                                    channels.extend(subscribe.channels);
                                    loop 
                                    {
                                        {
                                            let mut pubsub = env.pubsub.lock().unwrap();
                                            for channel in channels.drain(..) {
                                                let mut rx = match pubsub.get(&channel) {
                                                    Some(tx) => {
                                                        tx.subscribe()
                                                    }
                                                    None => {
                                                        let (tx, rx) = broadcast::channel(32);
                                                        pubsub.insert(channel.clone(), tx);
                                                        rx
                                                    }
                                                };
                                                let rx = Box::pin(async_stream::stream! {
                                                    loop {
                                                        match rx.recv().await {
                                                            Ok(msg) => yield msg,
                                                            Err(broadcast::error::RecvError::Lagged(_)) => {},
                                                            Err(_) => break,
                                                        }
                                                    }
                                                });
                                                stream_map.insert(channel.clone(), rx);
                                            }
                                        }
                                        

                                        select! {
                                            Some((channel, msg)) = stream_map.next() => {
                                                client.send(Frame::Array(vec![
                                                    Frame::Bulk("message".into()),
                                                    Frame::Bulk(channel.into()),
                                                    Frame::Bulk(msg),
                                                ])).await.unwrap();
                                            }
                                            Some(Ok((_, frame))) = client.next() => {
                                                match Command::from(frame) {
                                                    Ok(Command::Subscribe(subscribe)) => {
                                                        channels.extend(subscribe.channels);   
                                                    }
                                                    Ok(Command::Unsubscribe(mut unsubscribe)) => {
                                                        if unsubscribe.channels.is_empty() {
                                                            unsubscribe.channels = stream_map.keys().map(|s| s.to_string()).collect();
                                                        }
                                                        
                                                        for channel in unsubscribe.channels {
                                                            stream_map.remove(&channel);
                                                            client.send(Frame::Array(vec![
                                                                Frame::Bulk("unsubscribe".into()),
                                                                Frame::Bulk(channel.clone().into()),
                                                                Frame::Integer(stream_map.len() as i64),
                                                            ])).await.unwrap();
                                                        }
                                                    }
                                                    Ok(Command::Ping(_)) => {
                                                        client.send(Frame::Array(vec![
                                                            Frame::Bulk("pong".into()),
                                                            Frame::Bulk("".into()),
                                                        ])).await.unwrap();
                                                    }
                                                    _ => {}
                                                }
                                            }
                                        }
                                    }
                                },
                                Ok(Command::Publish(publish)) => {
                                    dbg!(&publish);
                                    let Publish{channel, message} = publish;
                                    let num_subscribers = {
                                        let pubsub = env.pubsub.lock().unwrap();
                                        pubsub.get(&channel).map(|tx| {
                                            tx.send(message).unwrap_or(0) 
                                        }).unwrap_or(0)
                                    };
                                    client.send(Frame::Integer(num_subscribers as i64)).await.unwrap();
                                }
                                Ok(Command::Zadd(zadd)) => {
                                    dbg!(&zadd);
                                    let Zadd{key, score, value} = zadd;
                                    let added = {
                                        let mut zsets = env.zsets.lock().unwrap();
                                        let zset = zsets.entry(key.clone()).or_default();
                                        let inserted = zset.insert((score, value));
                
                                        if inserted { 1 } else { 0 }
                                    };
                                    client.send(Frame::Integer(added)).await.unwrap();
                                },
                                Ok(Command::Zrank(zrank)) => {
                                    dbg!(&zrank);
                                    let Zrank{key, member} = zrank;
                                    let rank = {
                                        let zsets = env.zsets.lock().unwrap();
                                        if let Some(zset) = zsets.get(&key) {
                                            zset.rank(&member)
                                        } else {
                                            None
                                        }
                                    };
                                    let frame = match rank {
                                        Some(r) => Frame::Integer(r as i64),
                                        None => Frame::Null,
                                    };
                                    client.send(frame).await.unwrap();
                                },
                                Ok(Command::Zrange(zrange)) => {
                                    dbg!(&zrange);
                                    let Zrange{key, start, stop} = zrange;
                                    let members = {
                                        let zsets = env.zsets.lock().unwrap();
                                        if let Some(zset) = zsets.get(&key) {
                                            zset.range(start, stop).into_iter().map(|(_, member)| {
                                                member
                                            }).collect()
                                        } else {
                                            vec![]
                                        }
                                    };
                                    if members.is_empty() {
                                        client.send(Frame::Null).await.unwrap();
                                    } else {
                                        let frames = members.into_iter().map(|member| Frame::Bulk(member.into())).collect();
                                        client.send(Frame::Array(frames)).await.unwrap();
                                    }
                                }
                                Ok(Command::Zcard(zcard)) => {
                                    dbg!(&zcard);
                                    let Zcard{key} = zcard;
                                    let card = {
                                        let zsets = env.zsets.lock().unwrap();
                                        if let Some(zset) = zsets.get(&key) {
                                            zset.card()
                                        } else {
                                            0
                                        }
                                    };
                                    client.send(Frame::Integer(card as i64)).await.unwrap();
                                }
                                Ok(Command::Zscore(zscore)) => {
                                    dbg!(&zscore);
                                    let Zscore{key, member} = zscore;
                                    let score = {
                                        let zsets = env.zsets.lock().unwrap();
                                        if let Some(zset) = zsets.get(&key) {
                                            zset.score(&member)
                                        } else {
                                            None
                                        }
                                    };
                                    let frame = match score {
                                        Some(s) => Frame::Bulk(s.to_string().into()),
                                        None => Frame::Null,
                                    };
                                    client.send(frame).await.unwrap();
                                }
                                Ok(Command::Zrem(zrem)) => {
                                    dbg!(&zrem);
                                    let Zrem{key, member} = zrem;
                                    let removed = {
                                        let mut zsets = env.zsets.lock().unwrap();
                                        if let Some(zset) = zsets.get_mut(&key) {
                                            zset.remove(&member)
                                        } else {
                                            false
                                        }
                                    };
                                    let frame = if removed { Frame::Integer(1) } else { Frame::Integer(0) };
                                    client.send(frame).await.unwrap();
                                }
                                Ok(Command::Geoadd(geoadd)) => {
                                    dbg!(&geoadd);
                                    let Geoadd{key, longitude, latitude, member} = geoadd;
                                    if !(MIN_LONGITUDE..=MAX_LONGITUDE).contains(&longitude) || !(MIN_LATITUDE..=MAX_LATITUDE).contains(&latitude) {
                                        client.send(Frame::Error(format!("ERR invalid longitude,latitude pair {longitude:.6},{latitude:.6}"))).await.unwrap();
                                    } else {
                                        let added = {
                                            let mut geos = env.zsets.lock().unwrap();
                                            let geo = geos.entry(key.clone()).or_default();
                                            let score = cal_loc_score(longitude, latitude);
                                            let inserted = geo.insert((score, member));
                                            if inserted { 1 } else { 0 }
                                        };
                                        client.send(Frame::Integer(added)).await.unwrap();
                                    }
                                }
                                Ok(Command::Geopos(geopos)) => {
                                    dbg!(&geopos);
                                    let Geopos{key, members} = geopos;
                                    let locations = {
                                        let geos = env.zsets.lock().unwrap();
                                        if let Some(geo) = geos.get(&key) {
                                            members.iter().map(|member| geo.score(member).map(decode_coordinates)).collect::<Vec<_>>()
                                        } else {
                                            vec![None; members.len()]
                                        }
                                    };
                                    let frames = locations.iter().map(|loc| {
                                        if let Some((longitude, latitude)) = loc {
                                            Frame::Array(vec![
                                                Frame::Bulk(format!("{longitude}").into()),
                                                Frame::Bulk(format!("{latitude}").into()),
                                            ])
                                        } else {
                                            Frame::Array(vec![])
                                        }
                                    }).collect::<Vec<_>>();
                                    client.send(Frame::Array(frames)).await.unwrap();
                                }
                                Ok(Command::Geodist(geodist)) => {
                                    dbg!(&geodist);
                                    let Geodist{key, member1, member2} = geodist;
                                    let distance = {
                                        let geos = env.zsets.lock().unwrap();
                                        if let Some(geo) = geos.get(&key) {
                                            if let (Some(score1), Some(score2)) = (geo.score(&member1), geo.score(&member2)) {
                                                let loc1 = decode_coordinates(score1);
                                                let loc2 = decode_coordinates(score2);
                                                Some(cal_distance(loc1, loc2))
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    };
                                    let frame = if let Some(d) = distance {
                                        Frame::Bulk(format!("{d:.4}").into())
                                    } else {
                                        Frame::Null
                                    };
                                    client.send(frame).await.unwrap();
                                }
                                Ok(Command::Geosearch(geosearch)) => {
                                    dbg!(&geosearch);
                                    let Geosearch{key, longitude, latitude, radius, unit} = geosearch;
                                    let members = {
                                        let geos = env.zsets.lock().unwrap();
                                        if let Some(geo) = geos.get(&key) {
                                            search_within_radius(&geo.entries, (longitude, latitude), radius, &unit)
                                        } else {
                                            vec![]
                                        }
                                    };
                                    let frames = members.into_iter().map(|member| Frame::Bulk(member.into())).collect();
                                    client.send(Frame::Array(frames)).await.unwrap();
                                }

                                _ => unreachable!(),
                            }
                        }
                    }.await
                });
            }
            Err(e) => {
                println!("error: {e}");
            }
        }
    }
}

mod geo{

    pub const MIN_LATITUDE: f64 = -85.05112878;
    pub const MAX_LATITUDE: f64 = 85.05112878;
    pub const MIN_LONGITUDE: f64 = -180.0;
    pub const MAX_LONGITUDE: f64 = 180.0;

    pub const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
    pub const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

    pub const RADIUS : f64 = 6372797.560856; // In meters

    fn normalize(longitude: f64, latitude: f64) -> (f64, f64) {
        let normalized_longitude = 2.0f64.powi(26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;
        let normalized_latitude = 2.0f64.powi(26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
        (normalized_longitude, normalized_latitude)
    }

    fn spread(bits: u32) -> u64 {
        let mut x = bits as u64;
        x = (x | (x << 16)) & 0x0000FFFF0000FFFF;
        x = (x | (x <<  8)) & 0x00FF00FF00FF00FF;
        x = (x | (x <<  4)) & 0x0F0F0F0F0F0F0F0F;
        x = (x | (x <<  2)) & 0x3333333333333333;
        x = (x | (x <<  1)) & 0x5555555555555555;
        x
    }
    

    pub fn cal_loc_score(longitude: f64, latitude: f64) -> f64 {
        let (norm_lon, norm_lat) = normalize(longitude, latitude);
        let (norm_lon, norm_lat) = (norm_lon.floor() as u32, norm_lat.floor() as u32);
        let spread_lon = spread(norm_lon);
        let spread_lat = spread(norm_lat);
        (spread_lat | (spread_lon << 1)) as f64
    }

    fn compact(bits: u64) -> u32 {
        let mut x = bits & 0x5555555555555555;
        x = (x | (x >> 1)) & 0x3333333333333333;
        x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0F;
        x = (x | (x >> 4)) & 0x00FF00FF00FF00FF;
        x = (x | (x >> 8)) & 0x0000FFFF0000FFFF;
        x = (x | (x >> 16)) & 0x00000000FFFFFFFF;
        x as u32
    }

    pub fn decode_coordinates(score: f64) -> (f64, f64) {
        let latitude = score as u64;
        let longitude = score as u64 >> 1;

        let latitude = compact(latitude);
        let longitude = compact(longitude);

        let latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (latitude as f64 / 2.0f64.powi(26));
        let latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((latitude + 1) as f64 / 2.0f64.powi(26));
        let latitude = (latitude_min + latitude_max) / 2.0;

        let longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (longitude as f64 / 2.0f64.powi(26));
        let longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((longitude + 1) as f64 / 2.0f64.powi(26));
        let longitude = (longitude_min + longitude_max) / 2.0;
        
        (longitude, latitude)
    }

    pub fn cal_distance((lon1d, lat1d): (f64, f64), (lon2d, lat2d): (f64, f64)) -> f64 {
        let lon1r = lon1d.to_radians();
        let lon2r = lon2d.to_radians();
        let v = ((lon2r - lon1r) / 2.0).sin();

        let lat1r = lat1d.to_radians();
        let lat2r = lat2d.to_radians();
        let u = ((lat2r - lat1r) / 2.0).sin();

        let a = u * u + lat1r.cos() * lat2r.cos() * v * v;

        2.0 * RADIUS * a.sqrt().asin()
    }

    pub fn search_within_radius(
        candidates: &[(f64, String)],
        loc: (f64, f64),
        radius: f64,
        unit: &str,
    ) -> Vec<String> {
        todo!()
    }

    #[test]
    fn test_score_cal() {
        let longitude = 2.2944692;
        let latitude = 48.8584625;
        let score = cal_loc_score(longitude, latitude);
        assert_eq!(score, 3663832614298053.0);
    }

    #[test]
    fn test_decode() {
        let score = 3663832614298053.0;
        let (longitude, latitude) = decode_coordinates(score);
        assert!((longitude - 2.294471561908722).abs() < 0.0001);
        assert!((latitude - 48.85846255040141).abs() < 0.0001);
    }

    #[test]
    fn test_distance() {
        let loc1 = (11.5030378, 48.164271);
        let loc2 = (2.2944692, 48.8584625);
        let score1 = cal_loc_score(loc1.0, loc1.1);
        let score2 = cal_loc_score(loc2.0, loc2.1);
        let loc1 = decode_coordinates(score1);
        let loc2 = decode_coordinates(score2);
        let distance = cal_distance(loc1, loc2);
        assert!((distance - 682477.7582).abs() < 0.001);
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
            let millis = millis.parse::<u128>().unwrap();
            let mut seq = if millis == 0 { 1 } else { 0 };
            if let Some((last_millis, last_seq)) = last_id {
                if millis == last_millis {
                    seq = last_seq + 1;
                }
            }
            Ok((millis, seq))
        }
        Some((millis, seq)) => {
            let millis = millis.parse::<u128>().unwrap();
            let seq = seq.parse::<u64>().unwrap();
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
