use std::time::Instant;

use crate::{frame::Frame, parse::Parse, Env};

pub struct Get {
    pub key: String,
}

impl Get {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        
        Ok(Self {
            key
        })
    }
    
    pub async fn exec(&self, env: Env) -> Frame {
        let value = env.db.lock().unwrap().get(&self.key).cloned();
        if let Some((value, expires)) = value {
            if let Some(expires) = expires {
                if Instant::now() > expires {
                    return Frame::Null;
                }
            }

            return Frame::Bulk(value.clone().into());
        }

        Frame::Null
    }
}
