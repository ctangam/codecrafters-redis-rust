#[derive(Debug)]
pub struct Geopos {
    pub key: String,
    pub members: Vec<String>,
}

impl Geopos {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Geopos> {
        let key = parse.next_string()?;
        let mut members = Vec::new();
        while let Ok(member) = parse.next_string() {
            members.push(member);
        }
        Ok(Geopos { key, members })
    }
}