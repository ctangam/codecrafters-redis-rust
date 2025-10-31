use std::fmt::Display;

use async_trait::async_trait;
use config_get::ConfigGet;
use discard::Discard;
use echo::Echo;
use exec::Exec;
use get::Get;
use incr::Incr;
use info::Info;
use keys::Keys;
use multi::Multi;
use ping::Ping;
use psync::Psync;
use replconf::Replconf;
use rtype::Rtype;
use set::Set;
use unknown::Unknown;
use wait::Wait;
use xadd::Xadd;
use xrange::Xrange;
use xread::Xread;

use crate::{
    cmd::{
        blpop::Blpop, geoadd::Geoadd, geodist::Geodist, geopos::Geopos, geosearch::Geosearch,
        llen::Llen, lpop::Lpop, lpush::Lpush, lrange::Lrange, publish::Publish, rpush::Rpush,
        subscribe::Subscribe, unsubscribe::Unsubscribe, zadd::Zadd, zcard::Zcard, zrange::Zrange,
        zrank::Zrank, zrem::Zrem, zscore::Zscore,
    },
    env::Env,
    frame::Frame,
    parse::Parse,
};

pub mod blpop;
pub mod config_get;
pub mod discard;
pub mod echo;
pub mod exec;
pub mod geoadd;
pub mod geodist;
pub mod geopos;
pub mod geosearch;
pub mod get;
pub mod incr;
pub mod info;
pub mod keys;
pub mod llen;
pub mod lpop;
pub mod lpush;
pub mod lrange;
pub mod multi;
pub mod ping;
pub mod psync;
pub mod publish;
pub mod replconf;
pub mod rpush;
pub mod rtype;
pub mod set;
pub mod subscribe;
pub mod unknown;
pub mod unsubscribe;
pub mod wait;
pub mod xadd;
pub mod xrange;
pub mod xread;
pub mod zadd;
pub mod zcard;
pub mod zrange;
pub mod zrank;
pub mod zrem;
pub mod zscore;

pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Set(Set),
    Get(Get),
    Unknown(Unknown),
    ConfigGet(ConfigGet),
    Keys(Keys),
    Info(Info),
    Replconf(Replconf),
    Psync(Psync),
    Wait(Wait),
    Rtype(Rtype),
    Xadd(Xadd),
    Xrange(Xrange),
    Xread(Xread),
    Incr(Incr),
    Multi(Multi),
    Exec(Exec),
    Discard(Discard),
    Rpush(Rpush),
    Lrange(Lrange),
    Lpush(Lpush),
    Llen(Llen),
    Lpop(Lpop),
    Blpop(Blpop),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Publish(Publish),
    Zadd(Zadd),
    Zrank(Zrank),
    Zrange(Zrange),
    Zcard(Zcard),
    Zscore(Zscore),
    Zrem(Zrem),
    Geoadd(Geoadd),
    Geopos(Geopos),
    Geodist(Geodist),
    Geosearch(Geosearch),
}

impl Command {
    pub fn from(frame: Frame) -> crate::Result<Self> {
        let mut parse = Parse::new(frame)?;
        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "ping" => Command::Ping(Ping),
            "echo" => Command::Echo(Echo {
                message: parse.next_string()?,
            }),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "config" => Command::ConfigGet(ConfigGet::parse_frames(&mut parse)?),
            "keys" => Command::Keys(Keys::parse_frames(&mut parse)?),
            "info" => Command::Info(Info::parse_frames(&mut parse)?),
            "replconf" => Command::Replconf(Replconf::parse_frames(&mut parse)?),
            "psync" => Command::Psync(Psync::parse_frames(&mut parse)?),
            "wait" => Command::Wait(Wait::parse_frames(&mut parse)?),
            "type" => Command::Rtype(Rtype::parse_frames(&mut parse)?),
            "xadd" => Command::Xadd(Xadd::parse_frames(&mut parse)?),
            "xrange" => Command::Xrange(Xrange::parse_frames(&mut parse)?),
            "xread" => Command::Xread(Xread::parse_frames(&mut parse)?),
            "incr" => Command::Incr(Incr::parse_frames(&mut parse)?),
            "multi" => Command::Multi(Multi),
            "exec" => Command::Exec(Exec),
            "discard" => Command::Discard(Discard),
            "rpush" => Command::Rpush(Rpush::parse_frames(&mut parse)?),
            "lrange" => Command::Lrange(Lrange::parse_frames(&mut parse)?),
            "lpush" => Command::Lpush(Lpush::parse_frames(&mut parse)?),
            "llen" => Command::Llen(Llen::parse_frames(&mut parse)?),
            "lpop" => Command::Lpop(Lpop::parse_frames(&mut parse)?),
            "blpop" => Command::Blpop(Blpop::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "zadd" => Command::Zadd(Zadd::parse_frames(&mut parse)?),
            "zrank" => Command::Zrank(Zrank::parse_frames(&mut parse)?),
            "zrange" => Command::Zrange(Zrange::parse_frames(&mut parse)?),
            "zcard" => Command::Zcard(Zcard::parse_frames(&mut parse)?),
            "zscore" => Command::Zscore(Zscore::parse_frames(&mut parse)?),
            "zrem" => Command::Zrem(Zrem::parse_frames(&mut parse)?),
            "geoadd" => Command::Geoadd(Geoadd::parse_frames(&mut parse)?),
            "geopos" => Command::Geopos(Geopos::parse_frames(&mut parse)?),
            "geodist" => Command::Geodist(Geodist::parse_frames(&mut parse)?),
            "geosearch" => Command::Geosearch(Geosearch::parse_frames(&mut parse)?),
            _ => {
                // The command is not recognized and an Unknown command is
                // returned.
                //
                // `return` is called here to skip the `finish()` call below. As
                // the command is not recognized, there is most likely
                // unconsumed fields remaining in the `Parse` instance.
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.finish()?;

        Ok(command)
    }
}

#[async_trait]
pub trait Executor {
    async fn exec(&self, env: Env) -> Frame;
}

#[async_trait]
impl Executor for Command {
    async fn exec(&self, env: Env) -> Frame {
        match self {
            Command::Set(set) => set.exec(env).await,
            Command::Get(get) => get.exec(env).await,
            Command::Incr(incr) => incr.exec(env).await,
            _ => todo!(),
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Ping(_) => write!(f, "PING"),
            Command::Echo(_) => write!(f, "ECHO"),
            Command::Set(_) => write!(f, "SET"),
            Command::Get(_) => write!(f, "GET"),
            Command::Incr(_) => write!(f, "INCR"),
            Command::Unknown(_) => write!(f, "UNKNOWN"),
            _ => write!(f, "UNIMPLEMENTED COMMAND"),
        }
    }
}
