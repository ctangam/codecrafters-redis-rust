use crate::parse::Parse;

pub struct Psync {
    pub repl_id: String,
    pub repl_offset: String,
}

impl Psync {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Psync> {
        let repl_id = parse.next_string()?;
        let repl_offset = parse.next_string()?;

        Ok(Psync {
            repl_id,
            repl_offset,
        })
    }
}