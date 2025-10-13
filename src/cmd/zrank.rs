#[derive(Debug)]
pub struct Zrank {
    pub key: String,
    pub member: String,
}

impl Zrank {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Zrank> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;
        Ok(Zrank { key, member })
    }
}