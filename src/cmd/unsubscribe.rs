#[derive(Debug)]
pub struct Unsubscribe {
    pub channels: Vec<String>,
}

impl Unsubscribe {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Unsubscribe> {
        let mut channels = Vec::new();
        while let Ok(s) = parse.next_string() {
            channels.push(s);
        }
        Ok(Unsubscribe { channels })
    }
}
