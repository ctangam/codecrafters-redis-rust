use crate::parse::Parse;

pub struct ConfigGet {
    pub key: String,
}

impl ConfigGet {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<ConfigGet> {
        parse.next_string()?;

        Ok(ConfigGet {
            key: parse.next_string()?,
        })
    }
}
