#[derive(Debug)]
pub struct Zscore {
    pub key: String,
    pub member: String,
}

impl Zscore {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;
        Ok(Self { key, member })
    }
}