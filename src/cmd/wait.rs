pub struct Wait {
    pub numreplicas: u64,
    pub timeout: u64,
}

impl Wait {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Wait> {
        Ok(Wait {
            numreplicas: parse.next_int()?,
            timeout: parse.next_int()?,
        })
    }
}
