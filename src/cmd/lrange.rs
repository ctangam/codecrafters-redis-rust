#[derive(Debug)]
pub struct Lrange {
    pub list_key: String,
    pub start: i64,
    pub stop: i64,
}

impl Lrange {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Lrange> {
        let list_key = parse.next_string()?;
        let start = parse.next_int()? as i64;
        let stop = parse.next_int()? as i64;
        Ok(Lrange {
            list_key,
            start,
            stop,
        })
    }
}
