#[derive(Debug)]
pub struct Zcard {
    pub key: String,
}

impl Zcard {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Zcard> {
        let key = parse.next_string()?;
        Ok(Zcard { key })
    }
}
