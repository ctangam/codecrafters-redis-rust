use crate::parse::Parse;

pub struct Rtype {
    pub key: String,
}

impl Rtype {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        Ok(Self {
            key: parse.next_string()?,
        })
    }
}
