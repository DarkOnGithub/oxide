use oxide_core::compression::{deflate, lz4, lzma};

mod lz4_tests {
    use super::*;

    fn decode_hex(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0);
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
            .collect()
    }

    fn round_trip_case(data: &[u8]) {
        let encoded = lz4::apply(data).expect("compress should succeed");
        let decoded = lz4::reverse(&encoded).expect("decompress should succeed");
        assert_eq!(decoded, data);
    }

    #[test]
    fn roundtrip_empty() {
        round_trip_case(b"");
    }

    #[test]
    fn roundtrip_small_literal_only() {
        round_trip_case(b"abc");
    }

    #[test]
    fn roundtrip_repetitive() {
        round_trip_case(&[b'a'; 32]);
    }

    #[test]
    fn roundtrip_incompressible() {
        let data: Vec<u8> = (0u8..=255).cycle().take(32 * 1024).collect();
        round_trip_case(&data);
    }

    #[test]
    fn roundtrip_large_mixed() {
        let mut data = Vec::with_capacity(1024 * 1024);
        for i in 0..(1024 * 64) {
            data.extend_from_slice(b"oxide-lz4-");
            data.push((i & 0xFF) as u8);
            data.push(((i * 3) & 0xFF) as u8);
            data.extend((0u8..=31).cycle().take(8));
        }
        round_trip_case(&data);
    }

    #[test]
    fn decode_legacy_fixture_vectors() {
        let fixtures: [(&str, Vec<u8>); 5] = [
            ("0000000000", Vec::new()),
            ("0300000030616263", b"abc".to_vec()),
            ("200000001f6101000660616161616161", vec![b'a'; 32]),
            (
                "170000006768656c6c6f200600602068656c6c6f",
                b"hello hello hello hello".to_vec(),
            ),
            (
                "00020000ff31000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f4000ffa8603a3b3c3d3e3f",
                (0u8..=63).cycle().take(512).collect(),
            ),
        ];

        for (encoded_hex, expected) in fixtures {
            let encoded = decode_hex(encoded_hex);
            let decoded = lz4::reverse(&encoded).expect("legacy vector must decode");
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn decode_rejects_short_prefix() {
        assert!(lz4::reverse(&[1, 2, 3]).is_err());
    }

    #[test]
    fn decode_rejects_truncated_stream() {
        let mut encoded = lz4::apply(b"this is a test payload").expect("compress should succeed");
        encoded.pop();
        assert!(lz4::reverse(&encoded).is_err());
    }

    #[test]
    fn decode_rejects_offset_zero() {
        let encoded = decode_hex("04000000000000");
        assert!(lz4::reverse(&encoded).is_err());
    }

    #[test]
    fn decode_rejects_offset_out_of_bounds() {
        let encoded = decode_hex("0400000010610200");
        assert!(lz4::reverse(&encoded).is_err());
    }

    #[test]
    fn decode_rejects_malformed_length_extension() {
        let encoded = decode_hex("01000000f0");
        assert!(lz4::reverse(&encoded).is_err());
    }

    #[test]
    fn decode_rejects_decoded_size_mismatch() {
        let encoded = decode_hex("0400000030616263");
        assert!(lz4::reverse(&encoded).is_err());
    }

    #[test]
    fn decode_rejects_nonterminal_trailing_bytes() {
        let encoded = decode_hex("0300000030616263ff");
        assert!(lz4::reverse(&encoded).is_err());
    }
}

mod lzma_tests {
    use super::*;

    fn roundtrip(data: &[u8]) {
        let encoded = lzma::apply(data).expect("encode should succeed");
        let decoded = lzma::reverse(&encoded).expect("decode should succeed");
        assert_eq!(decoded, data);
    }

    #[test]
    fn roundtrip_empty() {
        roundtrip(b"");
    }

    #[test]
    fn roundtrip_small_literal() {
        roundtrip(b"lzma-like");
    }

    #[test]
    fn roundtrip_repetitive() {
        roundtrip(&vec![b'A'; 8192]);
    }

    #[test]
    fn roundtrip_incompressible() {
        let data: Vec<u8> = (0u8..=255).cycle().take(64 * 1024).collect();
        roundtrip(&data);
    }

    #[test]
    fn roundtrip_large_mixed() {
        let mut data = Vec::with_capacity(512 * 1024);
        for i in 0..(64 * 1024) {
            data.extend_from_slice(b"oxide-lzma-like-");
            data.push((i & 0xFF) as u8);
            data.push(((i * 5) & 0xFF) as u8);
        }
        roundtrip(&data);
    }

    #[test]
    fn deterministic_encoding() {
        let data = b"deterministic payload deterministic payload deterministic payload";
        let a = lzma::apply(data).expect("encode");
        let b = lzma::apply(data).expect("encode");
        assert_eq!(a, b);
    }

    #[test]
    fn rejects_invalid_magic() {
        let mut encoded = lzma::apply(b"abc").expect("encode");
        encoded[4] ^= 0xAA;
        assert!(lzma::reverse(&encoded).is_err());
    }

    #[test]
    fn rejects_truncated() {
        let mut encoded = lzma::apply(b"payload payload payload").expect("encode");
        encoded.pop();
        assert!(lzma::reverse(&encoded).is_err());
    }

    #[test]
    fn rejects_payload_length_mismatch() {
        let mut encoded = lzma::apply(b"payload").expect("encode");
        let payload_len_pos = 4 + 4 + 1 + 1 + 4;
        encoded[payload_len_pos] ^= 0x7F;
        assert!(lzma::reverse(&encoded).is_err());
    }
}

mod deflate_tests {
    use super::*;

    fn roundtrip(data: &[u8]) {
        let encoded = deflate::apply(data).expect("encode should succeed");
        let decoded = deflate::reverse(&encoded).expect("decode should succeed");
        assert_eq!(decoded, data);
    }

    #[test]
    fn roundtrip_empty() {
        roundtrip(b"");
    }

    #[test]
    fn roundtrip_small_literal() {
        roundtrip(b"oxide");
    }

    #[test]
    fn roundtrip_repetitive() {
        roundtrip(&vec![b'a'; 4096]);
    }

    #[test]
    fn roundtrip_incompressible() {
        let data: Vec<u8> = (0u8..=255).cycle().take(32 * 1024).collect();
        roundtrip(&data);
    }

    #[test]
    fn roundtrip_large_mixed() {
        let mut data = Vec::with_capacity(512 * 1024);
        for i in 0..(64 * 1024) {
            data.extend_from_slice(b"oxide-deflate-like-");
            data.push((i & 0xFF) as u8);
            data.push(((i * 3) & 0xFF) as u8);
        }
        roundtrip(&data);
    }

    #[test]
    fn deterministic_encoding() {
        let data = b"deterministic payload deterministic payload deterministic payload";
        let a = deflate::apply(data).expect("encode");
        let b = deflate::apply(data).expect("encode");
        assert_eq!(a, b);
    }

    #[test]
    fn rejects_invalid_magic() {
        let mut encoded = deflate::apply(b"abc").expect("encode");
        encoded[4] ^= 0xFF;
        assert!(deflate::reverse(&encoded).is_err());
    }

    #[test]
    fn rejects_truncated_stream() {
        let mut encoded = deflate::apply(b"this is a payload").expect("encode");
        encoded.pop();
        assert!(deflate::reverse(&encoded).is_err());
    }

    #[test]
    fn rejects_trailing_garbage() {
        let mut encoded = deflate::apply(b"abcabcabcabcabc").expect("encode");
        encoded.push(0xFF);
        assert!(deflate::reverse(&encoded).is_err());
    }
}
