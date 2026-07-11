use bytes::BytesMut;
use nacelle_codec::MessageDecoder;
use nacelle_reference_protocol::LengthDelimitedProtocol;
use nacelle_tcp::{DecodedMessage, Protocol};
use proptest::prelude::*;

proptest! {
    #[test]
    fn random_frame_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let protocol = LengthDelimitedProtocol;
        let mut decoder = protocol.decoder(16 * 1024 * 1024);
        let mut buf = BytesMut::from(bytes.as_slice());
        let _ = decoder.decode(&mut buf);
    }

    #[test]
    fn valid_fragmented_frames_decode_only_after_complete_body(
        request_id in any::<u64>(),
        opcode in any::<u64>(),
        flags in any::<u32>(),
        body in proptest::collection::vec(any::<u8>(), 0..512),
        split in 0usize..512,
    ) {
        let protocol = LengthDelimitedProtocol;
        let frame = protocol
            .encode_request_frame(request_id, opcode, flags, &body)
            .expect("valid frame should encode");
        let split = split.min(frame.len());
        let mut buf = BytesMut::from(&frame[..split]);
        let mut decoder = protocol.decoder(16 * 1024 * 1024);

        if split < 24 {
            prop_assert!(decoder
                .decode(&mut buf)
                .expect("partial valid frame should not error")
                .is_none());
        }

        buf.extend_from_slice(&frame[split..]);
        let decoded = decoder
            .decode(&mut buf)
            .expect("complete valid frame should decode")
            .expect("complete frame head should decode");
        let decoded = match decoded {
            DecodedMessage::Request(decoded) => decoded,
            DecodedMessage::OneWay(decoded) => match decoded.request {},
        };

        prop_assert_eq!(decoded.request.request_id, request_id);
        prop_assert_eq!(decoded.request.opcode, opcode);
        prop_assert_eq!(decoded.request.flags, flags);
        prop_assert_eq!(decoded.body_len, body.len());
        prop_assert_eq!(buf.len(), body.len());
    }

    #[test]
    fn malformed_frame_lengths_are_rejected(length in 0u32..20) {
        let protocol = LengthDelimitedProtocol;
        let mut decoder = protocol.decoder(16 * 1024 * 1024);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(&[0_u8; 20]);

        let result = decoder.decode(&mut buf);
        prop_assert!(result.is_err());
    }
}
