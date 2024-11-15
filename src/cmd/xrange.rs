use crate::parse::Parse;

pub struct Xrange {
    pub stream_key: String,
    pub start: String,
    pub end: String,
}

impl Xrange {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let stream_key = parse.next_string()?;
        let start = parse.next_string()?;
        let end = parse.next_string()?;

        Ok(Self {
            stream_key,
            start,
            end,
        })
    }
}
