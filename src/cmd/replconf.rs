use crate::parse::Parse;

use super::{unknown::Unknown, Command};

pub struct Replconf {
    pub port: Option<u32>,
    pub capa: Option<String>,
}

impl Replconf {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Replconf> {
        let keyword = parse.next_string()?;
        match keyword.as_str() {
            "listening-port" => {
                let port = parse.next_int()? as u32;

                Ok(Replconf {
                    port: Some(port),
                    capa: None,
                })
            }

            "capa" => {
                let capa = parse.next_string()?;

                Ok(Replconf {
                    port: None,
                    capa: Some(capa),
                })
            }

            _ => Err(format!("unknown replconf keyword: {}", keyword).into()),
        }
    }
}
