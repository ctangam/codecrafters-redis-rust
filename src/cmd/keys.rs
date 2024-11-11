use crate::parse::Parse;

pub struct Keys {
    pub pattern: String,
}

impl Keys {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Keys> {
        let pattern = parse.next_string()?;

        Ok(Keys { pattern })
    }
}
