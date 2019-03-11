use std::convert::From;
use std::error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    MetaRootNotFound,
    NoLogFiles,
}

pub type Result<T> = result::Result<T, Error>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::MetaRootNotFound => write!(f, "Meta root not found"),
            Error::NoLogFiles => write!(f, "No logfiles in the current directory"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => err.description(),
            Error::MetaRootNotFound => "Meta root not found",
            Error::NoLogFiles => "No log files",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Io(ref err) => Some(err),
            _ => None,
        }
    }
}
