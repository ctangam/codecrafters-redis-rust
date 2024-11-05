use std::{fmt, vec};

use bytes::Bytes;

use crate::frame::Frame;

pub struct Parse {
    parts: vec::IntoIter<Frame>,
}

/// Error encountered while parsing a frame.
///
/// Only `EndOfStream` errors are handled at runtime. All other errors result in
/// the connection being terminated.
#[derive(Debug)]
pub(crate) enum ParseError {
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
                parts: parts.into_iter(),
            }),
            frame => Err(format!("protocol error; expected array, got {:?}", frame).into()),
        }
    }

    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts
            .next()
            .ok_or(ParseError::EndOfStream)
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