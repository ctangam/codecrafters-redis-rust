#[derive(Debug)]
pub struct Lpop {
    pub list_key: String,
    pub count: Option<usize>,
}

impl Lpop {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Lpop> {
        let list_key = parse.next_string()?;
        let count = match parse.next_int() {
            Ok(i) => Some(i as usize),
            Err(_) => None,
        };
        Ok(Lpop { list_key, count })
    }
}
