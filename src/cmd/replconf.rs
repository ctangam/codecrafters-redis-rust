use crate::parse::Parse;

pub struct Replconf {
    pub port: Option<u32>,
    pub capa: Option<String>,
    pub ack: Option<String>,
}

impl Replconf {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Replconf> {
        let keyword = parse.next_string()?.to_lowercase();
        match keyword.as_str() {
            "listening-port" => {
                let port = parse.next_int()? as u32;

                Ok(Replconf {
                    port: Some(port),
                    capa: None,
                    ack: None,
                })
            }

            "capa" => {
                let capa = parse.next_string()?;

                Ok(Replconf {
                    port: None,
                    capa: Some(capa),
                    ack: None,
                })
            }

            "getack" => {
                let ack = parse.next_string()?;

                Ok(Replconf {
                    port: None,
                    capa: None,
                    ack: Some(ack),
                })
            }

            _ => Err(format!("unknown replconf keyword: {}", keyword).into()),
        }
    }
}
