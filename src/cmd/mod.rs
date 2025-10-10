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

use crate::{cmd::{blpop::Blpop, llen::Llen, lpop::Lpop, lpush::Lpush, lrange::Lrange, rpush::Rpush}, frame::Frame, parse::Parse, Env};

pub mod config_get;
pub mod echo;
pub mod exec;
pub mod get;
pub mod incr;
pub mod info;
pub mod keys;
pub mod multi;
pub mod ping;
pub mod psync;
pub mod replconf;
pub mod rtype;
pub mod set;
pub mod unknown;
pub mod wait;
pub mod xadd;
pub mod xrange;
pub mod xread;
pub mod discard;
pub mod rpush;
pub mod lrange;
pub mod lpush;
pub mod llen;
pub mod lpop;
pub mod blpop;

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
            Command::Ping(ping) => todo!(),
            Command::Echo(echo) => todo!(),
            Command::Set(set) => set.exec(env).await,
            Command::Get(get) => get.exec(env).await,
            Command::Unknown(unknown) => todo!(),
            Command::ConfigGet(config_get) => todo!(),
            Command::Keys(keys) => todo!(),
            Command::Info(info) => todo!(),
            Command::Replconf(replconf) => todo!(),
            Command::Psync(psync) => todo!(),
            Command::Wait(wait) => todo!(),
            Command::Rtype(rtype) => todo!(),
            Command::Xadd(xadd) => todo!(),
            Command::Xrange(xrange) => todo!(),
            Command::Xread(xread) => todo!(),
            Command::Incr(incr) => incr.exec(env).await,
            Command::Multi(multi) => todo!(),
            Command::Exec(exec) => todo!(),
            Command::Discard(discard) => todo!(),
            Command::Rpush(rpush) => todo!(),
            Command::Lrange(lrange) => todo!(),
            _ => todo!(),
        }
    }
}
