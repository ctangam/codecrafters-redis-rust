#[derive(Debug)]
pub struct Subscribe {
    pub channels: Vec<String>,
}

impl Subscribe {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Subscribe> {
        let mut channels = Vec::new();
        while let Ok(s) = parse.next_string() {
            channels.push(s);
        }
        Ok(Subscribe { channels })
    }
}