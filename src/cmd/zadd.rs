#[derive(Debug)]
pub struct Zadd {
    pub key: String,
    pub score: f64,
    pub value: String,
}

impl Zadd {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Zadd> {
        let key = parse.next_string()?;
        let score = parse.next_double()?;
        let value = parse.next_string()?;
        Ok(Zadd { key, score, value })
    }
}
