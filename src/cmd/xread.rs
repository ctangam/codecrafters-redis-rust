use crate::parse::Parse;

pub struct Xread {
    pub block_millis: Option<u64>,
    pub streams: Vec<(String, String)>,
}

impl Xread {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let tag = parse.next_string()?;
        let (block_millis, prefix) = if tag == "block" {
            let block_millis = parse.next_int()?;
            parse.next_string()?;
            (Some(block_millis), 4)
        } else {
            (None, 2)
        };
        let len = (parse.len() - prefix) / 2;

        let mut keys = Vec::new();
        for _ in 0..len {
            let stream_key = parse.next_string()?;
            keys.push(stream_key);
        }
        let mut ids = Vec::new();
        for _ in 0..len {
            let id = parse.next_string()?;
            ids.push(id);
        }

        let streams = keys.into_iter().zip(ids.into_iter()).collect();

        Ok(Self {
            block_millis,
            streams,
        })
    }
}
