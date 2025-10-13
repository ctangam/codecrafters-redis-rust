use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::{
    env::Env,
    frame::Frame,
    parse::{Parse, ParseError},
};

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

        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                let seconds = parse.next_int()?;
                expire = Some(Duration::from_secs(seconds));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                let milliseconds = parse.next_int()?;
                expire = Some(Duration::from_millis(milliseconds));
            }
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            Err(EndOfStream) => {}
            Err(err) => return Err(err.into()),
        };

        Ok(Set { key, value, expire })
    }

    pub async fn exec(&self, env: Env) -> Frame {
        let expires = self
            .expire
            .and_then(|expire| Instant::now().checked_add(expire));
        {
            let mut db = env.db.lock().unwrap();
            db.insert(self.key.clone(), (self.value.clone(), expires));
            drop(db);
        }

        Frame::Simple("OK".to_string())
    }
}
