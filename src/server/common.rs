use std::io::Error as IOError;

pub(crate) type Result<T> = std::result::Result<T, ErrorMsg>;

impl From<IOError> for ErrorMsg {
    fn from(e: IOError) -> Self {
        ErrorMsg::IoError(e)
    }
}

pub(crate) enum ErrorMsg {
    IoError(IOError)
}