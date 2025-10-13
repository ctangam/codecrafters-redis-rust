use crate::{frame::Frame, parse::Parse};

pub struct Incr {
    pub key: String,
}

impl Incr {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        Ok(Self { key })
    }

    pub async fn exec(&self, env: crate::env::Env) -> crate::frame::Frame {
        use atoi::atoi;
        let mut frame = Frame::Integer(1);
        env.db
            .lock()
            .unwrap()
            .entry(self.key.clone())
            .and_modify(|(v, _)| {
                if let Some(mut num) = atoi::<u64>(v) {
                    num += 1;
                    frame = Frame::Integer(num);
                    *v = num.to_string().into()
                } else {
                    frame = Frame::Error("ERR value is not an integer or out of range".into());
                }
            })
            .or_insert(("1".into(), None));
        frame
    }
}
