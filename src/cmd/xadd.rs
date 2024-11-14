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
        let mut pairs = Vec::new();
        while !parse.finish().is_ok() {
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
