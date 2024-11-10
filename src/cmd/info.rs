use crate::parse::Parse;

pub struct Info {
    pub replication: bool,
}

impl Info {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let replication = parse.next_string()? == "replication";
        Ok(Self { replication })
    }
}
