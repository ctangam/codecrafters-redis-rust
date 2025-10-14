#[derive(Debug)]
pub struct Lpush {
    pub list_key: String,
    pub elements: Vec<String>,
}

impl Lpush {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Lpush> {
        let list_key = parse.next_string()?;
        let mut elements = Vec::new();
        while let Ok(s) = parse.next_string() {
            elements.push(s.replace("\"", ""));
        }
        Ok(Lpush { list_key, elements })
    }
}
