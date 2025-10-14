#[derive(Debug)]
pub struct Rpush {
    pub list_key: String,
    pub elements: Vec<String>,
}

impl Rpush {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Rpush> {
        let list_key = parse.next_string()?;
        let mut elements = Vec::new();
        while let Ok(s) = parse.next_string() {
            elements.push(s.replace("\"", ""));
        }
        Ok(Rpush { list_key, elements })
    }
}
