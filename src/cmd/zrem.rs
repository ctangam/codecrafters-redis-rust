#[derive(Debug)]
pub struct Zrem {
    pub key: String,
    pub member: String,
}

impl Zrem {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Zrem> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;
        Ok(Self { key, member })
    }
}
