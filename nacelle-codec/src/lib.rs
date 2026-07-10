#![doc = include_str!("../README.md")]

pub mod decoder;
pub mod encoder;
pub mod io;
pub mod length_delimited;

pub use decoder::MessageDecoder;
pub use encoder::MessageEncoder;
#[cfg(feature = "buffer-rotation")]
pub use io::RotatingMessageReader;
pub use io::{MessageReadError, MessageReader, MessageWriteError, MessageWriter};
pub use length_delimited::{
    LengthDelimitedDecoder, LengthDelimitedEncodeError, LengthDelimitedEncoder,
    LengthDelimitedError,
};
