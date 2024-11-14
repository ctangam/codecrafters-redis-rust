use config_get::ConfigGet;
use echo::Echo;
use get::Get;
use info::Info;
use keys::Keys;
use ping::Ping;
use psync::Psync;
use replconf::Replconf;
use rtype::Rtype;
use set::Set;
use unknown::Unknown;
use wait::Wait;

use crate::{frame::Frame, parse::Parse};

pub mod config_get;
pub mod echo;
pub mod get;
pub mod info;
pub mod keys;
pub mod ping;
pub mod psync;
pub mod replconf;
pub mod rtype;
pub mod set;
pub mod unknown;
pub mod wait;

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
}

impl Command {
    pub fn from(frame: Frame) -> crate::Result<Self> {
        let mut parse = Parse::new(frame)?;
        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "ping" => Command::Ping(Ping {}),
            "echo" => Command::Echo(Echo {
                message: parse.next_string()?,
            }),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "get" => Command::Get(Get {
                key: parse.next_string()?,
            }),
            "config" => Command::ConfigGet(ConfigGet::parse_frames(&mut parse)?),
            "keys" => Command::Keys(Keys::parse_frames(&mut parse)?),
            "info" => Command::Info(Info::parse_frames(&mut parse)?),
            "replconf" => Command::Replconf(Replconf::parse_frames(&mut parse)?),
            "psync" => Command::Psync(Psync::parse_frames(&mut parse)?),
            "wait" => Command::Wait(Wait::parse_frames(&mut parse)?),
            "type" => Command::Rtype(Rtype::parse_frames(&mut parse)?),
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
