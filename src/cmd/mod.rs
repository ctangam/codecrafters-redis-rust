use echo::Echo;
use ping::Ping;

pub mod ping;
pub mod echo;

pub enum Command {
    Ping(Ping),
    Echo(Echo),
}

