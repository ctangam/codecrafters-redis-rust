use crate::parse::Parse;

pub struct Psync {
    pub repl_id: u64,
    pub repl_offset: u64,
}

impl Psync {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Psync> {
        let repl_id = parse.next_int()?;
        let repl_offset = parse.next_int()?;

        Ok(Psync {
            repl_id,
            repl_offset,
        })
    }
}