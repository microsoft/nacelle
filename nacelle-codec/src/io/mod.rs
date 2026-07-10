//! Asynchronous message I/O.

mod reader;
#[cfg(feature = "buffer-rotation")]
mod rotating;
mod writer;

pub use reader::{MessageReadError, MessageReader};
#[cfg(feature = "buffer-rotation")]
pub use rotating::RotatingMessageReader;
pub use writer::{MessageWriteError, MessageWriter};

#[cfg(test)]
mod tests;
