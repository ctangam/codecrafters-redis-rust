#[derive(Debug)]
pub struct Geodist {
    pub key: String,
    pub member1: String,
    pub member2: String,
}

impl Geodist {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Geodist> {
        let key = parse.next_string()?;
        let member1 = parse.next_string()?;
        let member2 = parse.next_string()?;
        Ok(Geodist {
            key,
            member1,
            member2,
        })
    }
}