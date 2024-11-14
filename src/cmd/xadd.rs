use bytes::Bytes;

use crate::parse::Parse;

pub struct Xadd {
    pub stream_key: String,
    pub id: String,
    pub pairs: Vec<(String, Bytes)>,
}

impl Xadd {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let stream_key = parse.next_string()?;

        let id = parse.next_string()?;
        let mut pairs = Vec::with_capacity(parse.len() - 3);
        for _ in 0..(parse.len() - 3) / 2 {
            let key = parse.next_string()?;
            let value = parse.next_bytes()?;
            pairs.push((key, value));
        }

        Ok(Self {
            stream_key,
            id,
            pairs,
        })
    }
}
