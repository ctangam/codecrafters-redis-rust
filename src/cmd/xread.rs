use crate::parse::Parse;

pub struct Xread {
    pub tag: String,
    pub streams: Vec<(String, String)>,
}

impl Xread {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let tag = parse.next_string()?;
        let len = (parse.len() - 2) / 2;

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

        Ok(Self { tag, streams })
    }
}
