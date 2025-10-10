#[derive(Debug)]
pub struct Rpush {
    pub list_key: String, 
    pub element: String,
}

impl Rpush {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Rpush> {
        Ok(Rpush {
            list_key: parse.next_string()?,
            element: parse.next_string()?.replace("\"", ""),
        })
    }
}