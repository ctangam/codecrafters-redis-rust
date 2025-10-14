#[derive(Debug)]
pub struct Geoadd {
    pub key: String,
    pub longitude: f64,
    pub latitude: f64,
    pub member: String,
}

impl Geoadd {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Geoadd> {
        let key = parse.next_string()?;
        let longitude = parse.next_double()?;
        let latitude = parse.next_double()?;
        let member = parse.next_string()?;
        Ok(Geoadd {
            key,
            longitude,
            latitude,
            member,
        })
    }
}
