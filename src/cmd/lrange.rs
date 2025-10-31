#[derive(Debug)]
pub struct Lrange {
    pub list_key: String,
    pub start: isize,
    pub stop: isize,
}

impl Lrange {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Lrange> {
        let list_key = parse.next_string()?;
        let start = parse.next_int()? as isize;
        let stop = parse.next_int()? as isize;
        Ok(Lrange {
            list_key,
            start,
            stop,
        })
    }
}
