#[derive(Debug)]
pub struct Geosearch {
    pub key: String,
    pub longitude: f64,
    pub latitude: f64,
    pub radius: f64,
    pub unit: String,
}

impl Geosearch {
    pub fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Geosearch> {
        let key = parse.next_string()?;
        assert_eq!(parse.next_string()?, "FROMLONLAT");
        let longitude = parse.next_double()?;
        let latitude = parse.next_double()?;
        assert_eq!(parse.next_string()?, "BYRADIUS");
        let radius = parse.next_double()?;
        let unit = parse.next_string()?;
        Ok(Geosearch {
            key,
            longitude,
            latitude,
            radius,
            unit,
        })
    }
}