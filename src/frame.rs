use core::str;

use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Double(f64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
    File(Bytes),
}

pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = (usize, Frame);
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let frame_type = src[0];
        let frame = match frame_type {
            b'+' => {
                let mut buffer = Vec::new();
                let mut i = 1;
                loop {
                    if src[i] == b'\r' && src[i + 1] == b'\n' {
                        break;
                    }
                    buffer.push(src[i]);
                    i += 1;
                }

                src.advance(i + 2);
                (i + 2, Frame::Simple(String::from_utf8(buffer).unwrap()))
            }
            b'-' => {
                let mut buffer = Vec::new();
                let mut i = 1;
                loop {
                    if src[i] == b'\r' && src[i + 1] == b'\n' {
                        break;
                    }
                    buffer.push(src[i]);
                    i += 1;
                }

                src.advance(i + 2);
                (i + 2, Frame::Error(String::from_utf8(buffer).unwrap()))
            }
            b':' => {
                let mut buffer = Vec::new();
                let mut i = 1;
                loop {
                    if src[i] == b'\r' && src[i + 1] == b'\n' {
                        break;
                    }
                    buffer.push(src[i]);
                    i += 1;
                }

                src.advance(i + 2);
                (
                    i + 2,
                    Frame::Integer(
                        u64::from_str_radix(str::from_utf8(&buffer).unwrap(), 10).unwrap(),
                    ),
                )
            }
            b',' => {
                let mut buffer = Vec::new();
                let mut i = 1;
                loop {
                    if src[i] == b'\r' && src[i + 1] == b'\n' {
                        break;
                    }
                    buffer.push(src[i]);
                    i += 1;
                }

                src.advance(i + 2);
                (
                    i + 2,
                    Frame::Double(f64::from_be_bytes(buffer.try_into().unwrap())),
                )
            }
            b'$' => {
                let mut buffer = Vec::new();
                let mut i = 1;
                loop {
                    if src[i] == b'\r' && src[i + 1] == b'\n' {
                        break;
                    }
                    buffer.push(src[i]);
                    i += 1;
                }

                let len = usize::from_str_radix(str::from_utf8(&buffer).unwrap(), 10).unwrap();

                let mut buffer = vec![0; len];
                buffer.copy_from_slice(&src[i + 2..i + 2 + len]);

                if src.remaining() > i + 2 + len
                    && src[i + 2 + len] == b'\r'
                    && src[i + 2 + len + 1] == b'\n'
                {
                    src.advance(i + 2 + len + 2);
                    (i + 2 + len + 2, Frame::Bulk(Bytes::from(buffer)))
                } else {
                    src.advance(i + 2 + len);
                    (i + 2 + len, Frame::Bulk(Bytes::from(buffer)))
                }
            }
            b'*' => {
                let mut buffer = Vec::new();
                let mut i = 1;
                loop {
                    if src[i] == b'\r' && src[i + 1] == b'\n' {
                        break;
                    }
                    buffer.push(src[i]);
                    i += 1;
                }

                let count = usize::from_str_radix(str::from_utf8(&buffer).unwrap(), 10).unwrap();
                src.advance(i + 2);

                let mut offset = i + 2;
                let mut frames = Vec::with_capacity(count);
                for _ in 0..count {
                    let frame = self.decode(src)?;
                    if let Some((n, frame)) = frame {
                        frames.push(frame);
                        offset += n;
                    }
                }

                (offset, Frame::Array(frames))
            }
            b'_' => {
                src.advance(3);
                (3, Frame::Null)
            }

            _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
        };

        Ok(Some(frame))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = std::io::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match frame {
            Frame::Simple(msg) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(msg.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Error(msg) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(msg.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Integer(num) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(num.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Double(num) => {
                dst.extend_from_slice(b",");
                dst.extend_from_slice(num.to_be_bytes().as_ref());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Bulk(msg) => {
                dst.extend_from_slice(b"$");
                let len = msg.len();
                dst.extend_from_slice(len.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(&msg[..]);
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Null => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            Frame::Array(frames) => {
                dst.extend_from_slice(b"*");
                let len = if frames.is_empty() { -1 } else { frames.len() as isize };
                dst.extend_from_slice(len.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for frame in frames {
                    self.encode(frame, dst)?;
                }
            }
            Frame::File(content) => {
                dst.extend_from_slice(b"$");
                let len = content.len();
                dst.extend_from_slice(len.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(&content[..]);
            }
        }
        Ok(())
    }
}
