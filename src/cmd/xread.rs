use crate::parse::Parse;

pub struct Xread {
    pub tag: String,
    pub stream_key: String,
    pub id: String,
}

impl Xread {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let tag = parse.next_string()?;
        let stream_key = parse.next_string()?;
        let id = parse.next_string()?;

        Ok(Self {
            tag,
            stream_key,
            id,
        })
    }
}
