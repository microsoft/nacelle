use std::convert::Infallible;

use bytes::BytesMut;
use nacelle::codec::MessageDecoder;

#[derive(Debug, Clone, Copy)]
struct LineDecoder;

impl MessageDecoder for LineDecoder {
    type Message = BytesMut;
    type Error = Infallible;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        let Some(newline_index) = input.iter().position(|byte| *byte == b'\n') else {
            return Ok(None);
        };

        let mut line = input.split_to(newline_index + 1);
        line.truncate(newline_index);

        Ok(Some(line))
    }
}

fn main() {
    let mut decoder = LineDecoder;
    let mut input = BytesMut::from(&b"hel"[..]);

    let original_len = input.len();
    let message = decoder.decode(&mut input).expect("decode cannot fail");

    assert!(message.is_none());
    assert_eq!(input.len(), original_len);
    println!("partial input remains buffered: {input:?}");

    input.extend_from_slice(b"lo\nworld\n");

    let message = decoder
        .decode(&mut input)
        .expect("decode cannot fail")
        .expect("a complete line is available");

    println!("decoded: {message:?}");
    println!("remaining: {input:?}")
}
