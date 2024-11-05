use echo::Echo;
use get::Get;
use ping::Ping;
use set::Set;

use crate::{frame::Frame, parse::{Parse, ParseError}};

pub mod ping;
pub mod echo;
pub mod set;
pub mod get;

pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Set(Set),
    Get(Get),
}

impl Command {
    pub fn from(frame: Frame) -> Result<Self, ParseError> {
        let mut parse = Parse::new(frame)?;
        let cmd = parse.next_string()?.to_lowercase();

        match cmd.as_str() {
            "ping" => Ok(Command::Ping(Ping {})),
            "echo" => Ok(Command::Echo(Echo {
                message: parse.next_string()?,
            })),
            "set" => Ok(Command::Set(Set {
                key: parse.next_string()?,
                value: parse.next_string()?,
            })),
            "get" => Ok(Command::Get(Get {
                key: parse.next_string()?,
            })),
            _ => Err(ParseError::Other("unknown command".into())),
        }
    }
}