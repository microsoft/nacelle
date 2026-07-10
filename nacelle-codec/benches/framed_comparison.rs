//! Comparative complete-message read benchmark.

use std::fmt::Debug;
use std::future::poll_fn;
use std::hint::black_box;
use std::pin::Pin;

use bytes::{Bytes, BytesMut};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use futures_core::Stream;
#[cfg(feature = "buffer-rotation")]
use nacelle_codec::RotatingMessageReader;
use nacelle_codec::{LengthDelimitedDecoder, LengthDelimitedError, MessageDecoder, MessageReader};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio_util::codec::{Decoder, FramedRead, LengthDelimitedCodec};

const MESSAGE_COUNT: usize = 64;
const PAYLOAD_LEN: usize = 64;
const MAX_FRAME_LEN: usize = 1024;

fn encoded_messages() -> Vec<u8> {
    let mut encoded = Vec::with_capacity(MESSAGE_COUNT * (4 + PAYLOAD_LEN));
    let payload_len = u32::try_from(PAYLOAD_LEN).expect("benchmark payload length");
    for value in 0..MESSAGE_COUNT {
        let value = u8::try_from(value).expect("benchmark message index");
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend(std::iter::repeat_n(value, PAYLOAD_LEN));
    }
    encoded
}

async fn loaded_duplex(encoded: &[u8]) -> DuplexStream {
    let (mut sender, receiver) = tokio::io::duplex(encoded.len());
    sender.write_all(encoded).await.expect("benchmark write");
    sender.shutdown().await.expect("benchmark shutdown");
    receiver
}

#[derive(Debug, Clone, Copy)]
struct DetachedDecoder(LengthDelimitedDecoder);

impl DetachedDecoder {
    const fn new(max_frame_len: usize) -> Self {
        Self(LengthDelimitedDecoder::new(max_frame_len))
    }
}

impl MessageDecoder for DetachedDecoder {
    type Message = Bytes;
    type Error = LengthDelimitedError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        self.0
            .decode(input)
            .map(|message| message.map(|message| Bytes::copy_from_slice(&message)))
    }
}

async fn read_with_nacelle<D>(encoded: &[u8], decoder: D)
where
    D: MessageDecoder,
    D::Error: Debug,
{
    let transport = loaded_duplex(encoded).await;
    let mut reader = MessageReader::new(transport, decoder);
    for _ in 0..MESSAGE_COUNT {
        let message = reader
            .read_message()
            .await
            .expect("benchmark decode")
            .expect("benchmark message");
        black_box(message);
    }
    assert!(
        reader
            .read_message()
            .await
            .expect("benchmark EOF")
            .is_none()
    );
}

fn framed_read_comparison(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("benchmark runtime");
    let encoded = encoded_messages();
    let mut group = c.benchmark_group("framed_read_64x64_bytes");
    group.throughput(Throughput::Bytes(
        u64::try_from(MESSAGE_COUNT * PAYLOAD_LEN).expect("benchmark byte count"),
    ));

    group.bench_function("nacelle_codec", |b| {
        b.to_async(&runtime).iter(|| async {
            read_with_nacelle(&encoded, LengthDelimitedDecoder::new(MAX_FRAME_LEN)).await;
        });
    });

    #[cfg(feature = "buffer-rotation")]
    group.bench_function("nacelle_codec_buffer_rotation", |b| {
        b.to_async(&runtime).iter(|| async {
            let transport = loaded_duplex(&encoded).await;
            let reader = MessageReader::new(transport, LengthDelimitedDecoder::new(MAX_FRAME_LEN));
            let mut reader = RotatingMessageReader::new(reader, 64 * 1024);
            for _ in 0..MESSAGE_COUNT {
                let message = reader
                    .read_message()
                    .await
                    .expect("benchmark decode")
                    .expect("benchmark message");
                black_box(message);
            }
            assert!(
                reader
                    .read_message()
                    .await
                    .expect("benchmark EOF")
                    .is_none()
            );
        });
    });

    group.bench_function("nacelle_codec_detached_wrapper", |b| {
        b.to_async(&runtime).iter(|| async {
            read_with_nacelle(&encoded, DetachedDecoder::new(MAX_FRAME_LEN)).await;
        });
    });

    group.bench_function("tokio_util_framed_read", |b| {
        b.to_async(&runtime).iter(|| async {
            let transport = loaded_duplex(&encoded).await;
            let codec = LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME_LEN)
                .new_codec();
            let mut reader = FramedRead::new(transport, codec);
            for _ in 0..MESSAGE_COUNT {
                let message = poll_fn(|context| Pin::new(&mut reader).poll_next(context))
                    .await
                    .expect("benchmark message")
                    .expect("benchmark decode");
                black_box(message);
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

fn decode_comparison(c: &mut Criterion) {
    let encoded = encoded_messages();
    let mut group = c.benchmark_group("decode_64x64_bytes");
    group.throughput(Throughput::Bytes(
        u64::try_from(MESSAGE_COUNT * PAYLOAD_LEN).expect("benchmark byte count"),
    ));

    group.bench_function("nacelle_codec", |b| {
        b.iter(|| {
            let mut input = BytesMut::from(encoded.as_slice());
            let mut decoder = LengthDelimitedDecoder::new(MAX_FRAME_LEN);
            for _ in 0..MESSAGE_COUNT {
                let message = decoder
                    .decode(&mut input)
                    .expect("benchmark decode")
                    .expect("benchmark message");
                black_box(message);
            }
        });
    });

    group.bench_function("nacelle_codec_buffered_reader", |b| {
        b.iter(|| {
            let mut reader = MessageReader::with_buffer(
                tokio::io::empty(),
                LengthDelimitedDecoder::new(MAX_FRAME_LEN),
                BytesMut::from(encoded.as_slice()),
            );
            for _ in 0..MESSAGE_COUNT {
                let message = reader
                    .decode_buffered()
                    .expect("benchmark decode")
                    .expect("benchmark message");
                black_box(message);
            }
        });
    });

    group.bench_function("tokio_util_decoder", |b| {
        b.iter(|| {
            let mut input = BytesMut::from(encoded.as_slice());
            let mut codec = LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME_LEN)
                .new_codec();
            for _ in 0..MESSAGE_COUNT {
                let message = codec
                    .decode(&mut input)
                    .expect("benchmark decode")
                    .expect("benchmark message");
                black_box(message);
            }
        });
    });

    group.finish();
}

fn fragmented_decode_comparison(c: &mut Criterion) {
    let partial_header = [0_u8; 3];
    let mut group = c.benchmark_group("decode_need_more_3_byte_header");

    group.bench_function("nacelle_codec", |b| {
        b.iter_batched(
            || {
                (
                    LengthDelimitedDecoder::new(MAX_FRAME_LEN),
                    BytesMut::from(partial_header.as_slice()),
                )
            },
            |(mut decoder, mut input)| {
                black_box(decoder.decode(&mut input).expect("benchmark decode"));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("nacelle_codec_buffered_reader", |b| {
        b.iter_batched(
            || {
                MessageReader::with_buffer(
                    tokio::io::empty(),
                    LengthDelimitedDecoder::new(MAX_FRAME_LEN),
                    BytesMut::from(partial_header.as_slice()),
                )
            },
            |mut reader| {
                black_box(reader.decode_buffered().expect("benchmark buffered decode"));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

#[cfg(feature = "buffer-rotation")]
fn buffer_rotation(c: &mut Criterion) {
    const REPLACEMENT_CAPACITY: usize = 8 * 1024;
    const OVERSIZED_CAPACITY: usize = 256 * 1024;

    let mut group = c.benchmark_group("buffer_rotation");
    group.bench_function("empty_buffer_noop", |b| {
        let mut reader = MessageReader::with_capacity(
            tokio::io::empty(),
            LengthDelimitedDecoder::new(MAX_FRAME_LEN),
            REPLACEMENT_CAPACITY,
        );
        b.iter(|| {
            reader.rotate_empty_buffer(black_box(REPLACEMENT_CAPACITY));
        });
    });
    group.bench_function("replace_empty_256k_with_8k", |b| {
        b.iter_batched(
            || {
                MessageReader::with_buffer(
                    tokio::io::empty(),
                    LengthDelimitedDecoder::new(MAX_FRAME_LEN),
                    BytesMut::with_capacity(OVERSIZED_CAPACITY),
                )
            },
            |mut reader| {
                reader.rotate_empty_buffer(black_box(REPLACEMENT_CAPACITY));
                black_box(reader);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

#[cfg(feature = "buffer-rotation")]
criterion_group!(
    benches,
    framed_read_comparison,
    decode_comparison,
    fragmented_decode_comparison,
    buffer_rotation
);
#[cfg(not(feature = "buffer-rotation"))]
criterion_group!(
    benches,
    framed_read_comparison,
    decode_comparison,
    fragmented_decode_comparison
);
criterion_main!(benches);
