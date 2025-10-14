#[derive(Debug)]
pub struct Zrange {
    pub key: String,
    pub start: isize,
    pub stop: isize,
}

impl Zrange {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Zrange> {
        let key = parse.next_string()?;
        let start = parse.next_int()? as isize;
        let stop = parse.next_int()? as isize;
        Ok(Zrange { key, start, stop })
    }
}
