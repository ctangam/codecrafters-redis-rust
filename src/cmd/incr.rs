use crate::parse::Parse;

pub struct Incr {
    pub key: String,
}

impl Incr {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        Ok(Self { key })
    }
}
