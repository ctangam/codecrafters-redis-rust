use bytes::Bytes;

#[derive(Debug)]
pub struct Publish {
    pub channel: String,
    pub message: Bytes,
}

impl Publish {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Publish> {
        let channel = parse.next_string()?;
        let message = parse.next_bytes()?;
        Ok(Publish { channel, message })
    }
}
