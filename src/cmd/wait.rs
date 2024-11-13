pub struct Wait {
    pub numreplicas: u8,
    pub timeout: u64,
}

impl Wait {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Wait> {
        Ok(Wait {
            numreplicas: parse.next_int()? as u8,
            timeout: parse.next_int()?,
        })
    }
}
