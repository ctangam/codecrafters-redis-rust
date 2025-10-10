#[derive(Debug)]
pub struct Llen {
    pub list_key: String,
}

impl Llen {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Llen> {
        let list_key = parse.next_string()?;
        Ok(Llen { list_key })
    }
}