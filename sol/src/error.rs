use std::fmt;

use crate::codec::Oid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Invalid(&'static str),
    Capacity(&'static str),
    Missing(Oid),
    MalformedHfs { oid: Oid, reason: &'static str },
    Conflict,
    Http(u16),
    Transport(String),
    Usage(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Invalid(what) => write!(f, "invalid {what}"),
            Self::Capacity(what) => write!(f, "capacity exhausted: {what}"),
            Self::Missing(oid) => write!(f, "missing object {oid}"),
            Self::MalformedHfs { oid, reason } => {
                write!(f, "object {oid}: invalid HFS metadata ({reason})")
            }
            Self::Conflict => f.write_str("manifest changed concurrently; retry the pipeline"),
            Self::Http(status) => write!(f, "object storage returned HTTP {status}"),
            Self::Transport(message) | Self::Usage(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<hyper::Error> for Error {
    fn from(value: hyper::Error) -> Self {
        Self::Transport(value.to_string())
    }
}

impl From<http::Error> for Error {
    fn from(value: http::Error) -> Self {
        Self::Transport(value.to_string())
    }
}
