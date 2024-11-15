use crate::parse::Parse;

pub struct Xrange {
    pub stream_key: String,
    pub start: u128,
    pub end: u128,
}

impl Xrange {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let stream_key = parse.next_string()?;
        let start = parse.next_string()?;
        let start = u128::from_str_radix(&start, 10).unwrap();
        let end = parse.next_string()?;
        let end = u128::from_str_radix(&end, 10).unwrap();

        Ok(Self {
            stream_key,
            start,
            end,
        })
    }
}
