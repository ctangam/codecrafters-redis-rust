use std::time::Duration;

use bytes::Bytes;

use crate::parse::{Parse, ParseError};

pub struct Set {
    pub key: String,
    pub value: Bytes,
    pub expire: Option<Duration>,
}

impl Set {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;

        // The expiration is optional. If nothing else follows, then it is
        // `None`.
        let mut expire = None;

        match parse.next_string()? {
            s if s.to_uppercase() == "EX" => {
                let seconds = parse.next_int()?;
                expire = Some(Duration::from_secs(seconds));
            }
            s if s.to_uppercase() == "PX" => {
                let milliseconds = parse.next_int()?;
                expire = Some(Duration::from_millis(milliseconds));
            }
            _ => return Err("protocol error; invalid set command".into()),
        };

        Ok(Set {
            key,
            value,
            expire,
        })

    }
}