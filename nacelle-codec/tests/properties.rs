//! Generated codec safety and fragmentation tests.

use bytes::BytesMut;
use nacelle_codec::{
    LengthDelimitedDecoder, LengthDelimitedEncodeError, LengthDelimitedEncoder,
    LengthDelimitedError, MessageDecoder, MessageEncoder,
};
use proptest::prelude::*;

proptest! {
    #[test]
    fn length_delimited_round_trip_survives_arbitrary_fragmentation(
        messages in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..256), 0..32),
        fragment_size in 1_usize..64,
    ) {
        let mut wire = BytesMut::new();
        let mut encoder = LengthDelimitedEncoder::new(256);
        for message in &messages {
            encoder.encode(message, &mut wire).expect("generated encode");
        }

        let mut decoder = LengthDelimitedDecoder::new(256);
        let mut input = BytesMut::new();
        let mut decoded = Vec::new();
        for fragment in wire.chunks(fragment_size) {
            input.extend_from_slice(fragment);
            while let Some(message) = decoder.decode(&mut input).expect("generated decode") {
                decoded.push(message.to_vec());
            }
        }

        prop_assert_eq!(decoded, messages);
        prop_assert!(input.is_empty());
    }

    #[test]
    fn oversized_declared_lengths_are_rejected_without_consumption(
        declared in 33_u32..=u32::MAX,
        tail in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut wire = declared.to_be_bytes().to_vec();
        wire.extend_from_slice(&tail);
        let mut input = BytesMut::from(wire.as_slice());
        let mut decoder = LengthDelimitedDecoder::new(32);

        let error = decoder.decode(&mut input).expect_err("oversized frame");
        prop_assert!(
            matches!(error, LengthDelimitedError::FrameTooLarge { .. }),
            "expected an oversized frame error",
        );
        prop_assert_eq!(input.len(), wire.len());
    }

    #[test]
    fn oversized_payloads_fail_before_modifying_output(
        payload in prop::collection::vec(any::<u8>(), 33..256),
        prefix in prop::collection::vec(any::<u8>(), 0..16),
    ) {
        let mut output = BytesMut::from(prefix.as_slice());
        let before = output.clone();
        let error = LengthDelimitedEncoder::new(32)
            .encode(payload, &mut output)
            .expect_err("oversized payload");

        prop_assert!(
            matches!(error, LengthDelimitedEncodeError::FrameTooLarge { .. }),
            "expected an oversized payload error",
        );
        prop_assert_eq!(output, before);
    }
}
