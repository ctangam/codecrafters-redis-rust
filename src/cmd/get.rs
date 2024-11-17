use std::time::Instant;

use crate::{frame::Frame, Env};

pub struct Get {
    pub key: String,
}

impl Get {
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
