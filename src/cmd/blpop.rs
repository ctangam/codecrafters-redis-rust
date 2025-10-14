#[derive(Debug)]
pub struct Blpop {
    pub list_key: String,
    pub timeout: u64,
}

impl Blpop {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Blpop> {
        let list_key = parse.next_string()?;
        let timeout = parse.next_int()? as u64;
        Ok(Blpop { list_key, timeout })
    }
}
