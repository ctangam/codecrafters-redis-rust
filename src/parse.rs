use std::{fmt, vec};

use bytes::Bytes;

use crate::frame::Frame;

pub struct Parse {
    len: usize,
    parts: vec::IntoIter<Frame>,
}

/// Error encountered while parsing a frame.
///
/// Only `EndOfStream` errors are handled at runtime. All other errors result in
/// the connection being terminated.
#[derive(Debug)]
pub enum ParseError {
    /// Attempting to extract a value failed due to the frame being fully
    /// consumed.
    EndOfStream,

    /// All other errors
    Other(crate::Error),
}

impl Parse {
    pub fn new(frame: Frame) -> Result<Self, ParseError> {
        match frame {
            Frame::Array(parts) => Ok(Parse {
                len: parts.len(),
                parts: parts.into_iter(),
            }),
            frame => Err(format!("protocol error; expected array, got {:?}", frame).into()),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    pub fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Simple(msg) => Ok(msg),
            Frame::Bulk(msg) => Ok(String::from_utf8_lossy(&msg).to_string()),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    pub fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &str = "protocol error; invalid number";

        match self.next()? {
            // An integer frame type is already stored as an integer.
            Frame::Integer(v) => Ok(v),
            // Simple and bulk frames must be parsed as integers. If the parsing
            // fails, an error is returned.
            Frame::Simple(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error; expected int frame but got {:?}", frame).into()),
        }
    }

    pub fn next_double(&mut self) -> Result<f64, ParseError> {
        const MSG: &str = "protocol error; invalid double";

        match self.next()? {
            Frame::Double(v) => Ok(v),
            frame => {
                Err(format!("protocol error; expected double frame but got {:?}", frame).into())
            }
        }
    }

    pub fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            Frame::Simple(msg) => Ok(msg.into_bytes().into()),
            Frame::Bulk(msg) => Ok(msg),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    pub fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}
