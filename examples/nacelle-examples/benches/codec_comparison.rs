//! Codec comparison using one shared length-prefixed wire format.
//!
//! `actix-codec` re-exports Tokio Util's `Decoder` and `Encoder` traits. The
//! direct groups therefore compare Nacelle's trait with two distinct codec
//! types implementing that shared trait. The framed-read group compares the
//! distinct I/O adapters supplied by all three crates.

use std::fmt;
use std::future::poll_fn;
use std::hint::black_box;
use std::pin::Pin;

use actix_codec::{Decoder as ActixDecoder, Encoder as ActixEncoder, Framed as ActixFramed};
use bytes::{Buf, BytesMut};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures_core::Stream;
use nacelle::codec::{MessageDecoder, MessageEncoder, MessageReader};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio_util::codec::{Decoder as TokioDecoder, Encoder as TokioEncoder, FramedRead};

const HEADER_LEN: usize = 4;
const MESSAGE_COUNT: usize = 64;
const MAX_FRAME_LEN: usize = 8 * 1024;
const PAYLOAD_LENGTHS: [usize; 2] = [64, 4 * 1024];

#[derive(Debug, Clone, PartialEq, Eq)]
enum FrameError {
    FrameTooLarge { len: usize, max: usize },
    LengthOverflow,
    Io(std::io::ErrorKind),
}

impl fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge { len, max } => {
                write!(formatter, "frame length {len} exceeds limit {max}")
            }
            Self::LengthOverflow => formatter.write_str("frame length overflow"),
            Self::Io(kind) => write!(formatter, "I/O error: {kind:?}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<std::io::Error> for FrameError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.kind())
    }
}

fn decode_frame(
    input: &mut BytesMut,
    max_frame_len: usize,
) -> Result<Option<BytesMut>, FrameError> {
    let Some(header) = input.get(..HEADER_LEN) else {
        return Ok(None);
    };
    let length_bytes: [u8; HEADER_LEN] =
        header.try_into().map_err(|_| FrameError::LengthOverflow)?;
    let payload_len = u32::from_be_bytes(length_bytes) as usize;
    if payload_len > max_frame_len {
        return Err(FrameError::FrameTooLarge {
            len: payload_len,
            max: max_frame_len,
        });
    }
    let frame_len = HEADER_LEN
        .checked_add(payload_len)
        .ok_or(FrameError::LengthOverflow)?;
    if input.len() < frame_len {
        return Ok(None);
    }

    let mut payload = input.split_to(frame_len);
    payload.advance(HEADER_LEN);
    Ok(Some(payload))
}

fn encode_frame(
    payload: &[u8],
    output: &mut BytesMut,
    max_frame_len: usize,
) -> Result<(), FrameError> {
    if payload.len() > max_frame_len {
        return Err(FrameError::FrameTooLarge {
            len: payload.len(),
            max: max_frame_len,
        });
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| FrameError::LengthOverflow)?;
    output.reserve(HEADER_LEN.saturating_add(payload.len()));
    output.extend_from_slice(&payload_len.to_be_bytes());
    output.extend_from_slice(payload);
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct NacelleCodec {
    max_frame_len: usize,
}

impl MessageDecoder for NacelleCodec {
    type Message = BytesMut;
    type Error = FrameError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        decode_frame(input, self.max_frame_len)
    }
}

impl<'message> MessageEncoder<&'message [u8]> for NacelleCodec {
    type Error = FrameError;

    fn encode(
        &mut self,
        message: &'message [u8],
        output: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        encode_frame(message, output, self.max_frame_len)
    }
}

#[derive(Debug, Clone, Copy)]
struct TokioCodec {
    max_frame_len: usize,
}

impl TokioDecoder for TokioCodec {
    type Item = BytesMut;
    type Error = FrameError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        decode_frame(input, self.max_frame_len)
    }
}

impl<'message> TokioEncoder<&'message [u8]> for TokioCodec {
    type Error = FrameError;

    fn encode(
        &mut self,
        message: &'message [u8],
        output: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        encode_frame(message, output, self.max_frame_len)
    }
}

#[derive(Debug, Clone, Copy)]
struct ActixCodec {
    max_frame_len: usize,
}

impl ActixDecoder for ActixCodec {
    type Item = BytesMut;
    type Error = FrameError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        decode_frame(input, self.max_frame_len)
    }
}

impl<'message> ActixEncoder<&'message [u8]> for ActixCodec {
    type Error = FrameError;

    fn encode(
        &mut self,
        message: &'message [u8],
        output: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        encode_frame(message, output, self.max_frame_len)
    }
}

fn encoded_messages(payload_len: usize) -> BytesMut {
    let mut encoded = BytesMut::with_capacity(MESSAGE_COUNT * (HEADER_LEN + payload_len));
    let mut codec = NacelleCodec {
        max_frame_len: MAX_FRAME_LEN,
    };
    let payload = vec![0xAB; payload_len];
    for _ in 0..MESSAGE_COUNT {
        MessageEncoder::encode(&mut codec, payload.as_slice(), &mut encoded)
            .expect("benchmark fixture should encode");
    }
    encoded
}

fn assert_equivalent(payload_len: usize) {
    let encoded = encoded_messages(payload_len);
    let expected_payload = vec![0xAB; payload_len];

    let mut nacelle_input = encoded.clone();
    let mut tokio_input = encoded.clone();
    let mut actix_input = encoded;
    let mut nacelle = NacelleCodec {
        max_frame_len: MAX_FRAME_LEN,
    };
    let mut tokio = TokioCodec {
        max_frame_len: MAX_FRAME_LEN,
    };
    let mut actix = ActixCodec {
        max_frame_len: MAX_FRAME_LEN,
    };

    for _ in 0..MESSAGE_COUNT {
        let nacelle_message = MessageDecoder::decode(&mut nacelle, &mut nacelle_input)
            .expect("nacelle decode")
            .expect("nacelle message");
        let tokio_message = TokioDecoder::decode(&mut tokio, &mut tokio_input)
            .expect("tokio decode")
            .expect("tokio message");
        let actix_message = ActixDecoder::decode(&mut actix, &mut actix_input)
            .expect("actix decode")
            .expect("actix message");
        assert_eq!(nacelle_message, expected_payload.as_slice());
        assert_eq!(nacelle_message, tokio_message);
        assert_eq!(nacelle_message, actix_message);
    }
    assert!(nacelle_input.is_empty());
    assert!(tokio_input.is_empty());
    assert!(actix_input.is_empty());

    let mut nacelle_output = BytesMut::new();
    let mut tokio_output = BytesMut::new();
    let mut actix_output = BytesMut::new();
    MessageEncoder::encode(
        &mut nacelle,
        expected_payload.as_slice(),
        &mut nacelle_output,
    )
    .expect("nacelle encode");
    TokioEncoder::encode(&mut tokio, expected_payload.as_slice(), &mut tokio_output)
        .expect("tokio encode");
    ActixEncoder::encode(&mut actix, expected_payload.as_slice(), &mut actix_output)
        .expect("actix encode");
    assert_eq!(nacelle_output, tokio_output);
    assert_eq!(nacelle_output, actix_output);
}

async fn loaded_duplex(encoded: &[u8]) -> DuplexStream {
    let (mut sender, receiver) = tokio::io::duplex(encoded.len());
    sender
        .write_all(encoded)
        .await
        .expect("benchmark fixture write");
    sender.shutdown().await.expect("benchmark fixture shutdown");
    receiver
}

fn framed_read_comparison(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("benchmark runtime");

    for payload_len in PAYLOAD_LENGTHS {
        let encoded = encoded_messages(payload_len);
        let mut group =
            criterion.benchmark_group(format!("codec_framed_read_64x{payload_len}_bytes"));
        group.throughput(Throughput::Bytes(
            u64::try_from(MESSAGE_COUNT * payload_len).expect("benchmark byte count"),
        ));

        group.bench_function("nacelle_codec", |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                let transport = loaded_duplex(&encoded).await;
                let mut reader = MessageReader::new(
                    transport,
                    NacelleCodec {
                        max_frame_len: MAX_FRAME_LEN,
                    },
                );
                for _ in 0..MESSAGE_COUNT {
                    black_box(
                        reader
                            .read_message()
                            .await
                            .expect("nacelle read")
                            .expect("nacelle message"),
                    );
                }
                assert!(reader.read_message().await.expect("nacelle EOF").is_none());
            });
        });
        group.bench_function("tokio_util", |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                let transport = loaded_duplex(&encoded).await;
                let mut reader = FramedRead::new(
                    transport,
                    TokioCodec {
                        max_frame_len: MAX_FRAME_LEN,
                    },
                );
                for _ in 0..MESSAGE_COUNT {
                    black_box(
                        poll_fn(|context| Pin::new(&mut reader).poll_next(context))
                            .await
                            .expect("tokio message")
                            .expect("tokio read"),
                    );
                }
                assert!(
                    poll_fn(|context| Pin::new(&mut reader).poll_next(context))
                        .await
                        .is_none()
                );
            });
        });
        group.bench_function("actix_codec", |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                let transport = loaded_duplex(&encoded).await;
                let mut reader = ActixFramed::new(
                    transport,
                    ActixCodec {
                        max_frame_len: MAX_FRAME_LEN,
                    },
                );
                for _ in 0..MESSAGE_COUNT {
                    black_box(
                        poll_fn(|context| Pin::new(&mut reader).poll_next(context))
                            .await
                            .expect("actix message")
                            .expect("actix read"),
                    );
                }
                assert!(
                    poll_fn(|context| Pin::new(&mut reader).poll_next(context))
                        .await
                        .is_none()
                );
            });
        });
        group.finish();
    }
}

fn decode_comparison(criterion: &mut Criterion) {
    for payload_len in PAYLOAD_LENGTHS {
        assert_equivalent(payload_len);
        let encoded = encoded_messages(payload_len);
        let mut group = criterion.benchmark_group(format!("codec_decode_64x{payload_len}_bytes"));
        group.throughput(Throughput::Bytes(
            u64::try_from(MESSAGE_COUNT * payload_len).expect("benchmark byte count"),
        ));

        group.bench_function("nacelle_codec", |bencher| {
            bencher.iter_batched(
                || encoded.clone(),
                |mut input| {
                    let mut codec = NacelleCodec {
                        max_frame_len: MAX_FRAME_LEN,
                    };
                    for _ in 0..MESSAGE_COUNT {
                        black_box(
                            MessageDecoder::decode(&mut codec, &mut input)
                                .expect("nacelle decode")
                                .expect("nacelle message"),
                        );
                    }
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_function("tokio_util", |bencher| {
            bencher.iter_batched(
                || encoded.clone(),
                |mut input| {
                    let mut codec = TokioCodec {
                        max_frame_len: MAX_FRAME_LEN,
                    };
                    for _ in 0..MESSAGE_COUNT {
                        black_box(
                            TokioDecoder::decode(&mut codec, &mut input)
                                .expect("tokio decode")
                                .expect("tokio message"),
                        );
                    }
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_function("actix_codec", |bencher| {
            bencher.iter_batched(
                || encoded.clone(),
                |mut input| {
                    let mut codec = ActixCodec {
                        max_frame_len: MAX_FRAME_LEN,
                    };
                    for _ in 0..MESSAGE_COUNT {
                        black_box(
                            ActixDecoder::decode(&mut codec, &mut input)
                                .expect("actix decode")
                                .expect("actix message"),
                        );
                    }
                },
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }
}

fn encode_comparison(criterion: &mut Criterion) {
    for payload_len in PAYLOAD_LENGTHS {
        let payload = vec![0xAB; payload_len];
        let output_capacity = MESSAGE_COUNT * (HEADER_LEN + payload_len);
        let mut group = criterion.benchmark_group(format!("codec_encode_64x{payload_len}_bytes"));
        group.throughput(Throughput::Bytes(
            u64::try_from(MESSAGE_COUNT * payload_len).expect("benchmark byte count"),
        ));

        group.bench_function("nacelle_codec", |bencher| {
            bencher.iter(|| {
                let mut codec = NacelleCodec {
                    max_frame_len: MAX_FRAME_LEN,
                };
                let mut output = BytesMut::with_capacity(output_capacity);
                for _ in 0..MESSAGE_COUNT {
                    MessageEncoder::encode(&mut codec, black_box(payload.as_slice()), &mut output)
                        .expect("nacelle encode");
                }
                black_box(output);
            });
        });
        group.bench_function("tokio_util", |bencher| {
            bencher.iter(|| {
                let mut codec = TokioCodec {
                    max_frame_len: MAX_FRAME_LEN,
                };
                let mut output = BytesMut::with_capacity(output_capacity);
                for _ in 0..MESSAGE_COUNT {
                    TokioEncoder::encode(&mut codec, black_box(payload.as_slice()), &mut output)
                        .expect("tokio encode");
                }
                black_box(output);
            });
        });
        group.bench_function("actix_codec", |bencher| {
            bencher.iter(|| {
                let mut codec = ActixCodec {
                    max_frame_len: MAX_FRAME_LEN,
                };
                let mut output = BytesMut::with_capacity(output_capacity);
                for _ in 0..MESSAGE_COUNT {
                    ActixEncoder::encode(&mut codec, black_box(payload.as_slice()), &mut output)
                        .expect("actix encode");
                }
                black_box(output);
            });
        });
        group.finish();
    }
}

fn incomplete_decode_comparison(criterion: &mut Criterion) {
    let partial_header = [0_u8; HEADER_LEN - 1];
    let mut nacelle_input = BytesMut::from(partial_header.as_slice());
    let mut tokio_input = nacelle_input.clone();
    let mut actix_input = nacelle_input.clone();
    assert!(
        MessageDecoder::decode(
            &mut NacelleCodec {
                max_frame_len: MAX_FRAME_LEN,
            },
            &mut nacelle_input,
        )
        .expect("nacelle decode")
        .is_none()
    );
    assert!(
        TokioDecoder::decode(
            &mut TokioCodec {
                max_frame_len: MAX_FRAME_LEN,
            },
            &mut tokio_input,
        )
        .expect("tokio decode")
        .is_none()
    );
    assert!(
        ActixDecoder::decode(
            &mut ActixCodec {
                max_frame_len: MAX_FRAME_LEN,
            },
            &mut actix_input,
        )
        .expect("actix decode")
        .is_none()
    );
    assert_eq!(nacelle_input, partial_header.as_slice());
    assert_eq!(tokio_input, partial_header.as_slice());
    assert_eq!(actix_input, partial_header.as_slice());

    let mut group = criterion.benchmark_group("codec_decode_incomplete_3_byte_header");

    group.bench_with_input(
        BenchmarkId::from_parameter("nacelle_codec"),
        &partial_header,
        |bencher, input| {
            bencher.iter_batched(
                || BytesMut::from(input.as_slice()),
                |mut input| {
                    let mut codec = NacelleCodec {
                        max_frame_len: MAX_FRAME_LEN,
                    };
                    black_box(
                        MessageDecoder::decode(&mut codec, &mut input).expect("nacelle decode"),
                    );
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        },
    );
    group.bench_with_input(
        BenchmarkId::from_parameter("tokio_util"),
        &partial_header,
        |bencher, input| {
            bencher.iter_batched(
                || BytesMut::from(input.as_slice()),
                |mut input| {
                    let mut codec = TokioCodec {
                        max_frame_len: MAX_FRAME_LEN,
                    };
                    black_box(TokioDecoder::decode(&mut codec, &mut input).expect("tokio decode"));
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        },
    );
    group.bench_with_input(
        BenchmarkId::from_parameter("actix_codec"),
        &partial_header,
        |bencher, input| {
            bencher.iter_batched(
                || BytesMut::from(input.as_slice()),
                |mut input| {
                    let mut codec = ActixCodec {
                        max_frame_len: MAX_FRAME_LEN,
                    };
                    black_box(ActixDecoder::decode(&mut codec, &mut input).expect("actix decode"));
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        },
    );
    group.finish();
}

criterion_group!(
    benches,
    framed_read_comparison,
    decode_comparison,
    encode_comparison,
    incomplete_decode_comparison
);
criterion_main!(benches);
