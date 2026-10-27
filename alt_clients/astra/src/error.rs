use crate::Oid20;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Invalid(&'static str),
    Capacity(&'static str),
    Missing(Oid20),
    InvalidPointer(Oid20),
    Conflict,
    Git(i32),
    Http(u16),
    Transport(ureq::Error),
    Pack(crate::pack::PackError),
    Index(crate::idx::IdxError),
    Delta(crate::delta::DeltaError),
    Zlib(crate::zlib::ZError),
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O: {e}"),
            Self::Invalid(e) => write!(f, "invalid data: {e}"),
            Self::Capacity(e) => write!(f, "configured capacity exhausted: {e}"),
            Self::Missing(oid) => write!(f, "missing object {oid}"),
            Self::InvalidPointer(oid) => write!(f, "invalid Git-HFS pointer object {oid}"),
            Self::Conflict => f.write_str("manifest changed concurrently; retry the pipeline"),
            Self::Git(code) => write!(f, "Git exited with status {code} (see stderr)"),
            Self::Http(code) => write!(f, "object storage HTTP status {code}"),
            Self::Transport(_) => {
                f.write_str("object storage transport failure (credentials redacted)")
            }
            Self::Pack(e) => write!(f, "{e}"),
            Self::Index(e) => write!(f, "{e}"),
            Self::Delta(e) => write!(f, "{e}"),
            Self::Zlib(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}

macro_rules! conversion {
    ($from:ty, $variant:ident) => {
        impl From<$from> for Error {
            fn from(e: $from) -> Self {
                Self::$variant(e)
            }
        }
    };
}
conversion!(std::io::Error, Io);
conversion!(ureq::Error, Transport);
conversion!(crate::pack::PackError, Pack);
conversion!(crate::idx::IdxError, Index);
conversion!(crate::delta::DeltaError, Delta);
conversion!(crate::zlib::ZError, Zlib);

impl From<crate::buf::Overflow> for Error {
    fn from(_: crate::buf::Overflow) -> Self {
        Self::Capacity("byte buffer")
    }
}
