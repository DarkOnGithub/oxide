#[cfg(feature = "telemetry")]
mod telemetry_enabled_tests {
    use std::sync::Mutex;

    use oxide_core::compression::lz4;
    use oxide_core::telemetry;
    use oxide_core::telemetry::tags;

    static TELEMETRY_TEST_MUTEX: Mutex<()> = Mutex::new(());

    fn decode_hex(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0);
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
            .collect()
    }

    fn decode_case(hex: &str, expected: &[u8]) {
        let decoded = lz4::reverse(&decode_hex(hex)).expect("decode should succeed");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn lz4_decode_fast_path_metrics_are_recorded() {
        let _guard = TELEMETRY_TEST_MUTEX
            .lock()
            .expect("telemetry test lock poisoned");

        telemetry::reset();

        decode_case("0c00000017610100", &[b'a'; 12]);
        decode_case("0c0000002661620200", b"abababababab");
        decode_case("1000000048616263640400", b"abcdabcdabcdabcd");
        decode_case(
            "180000008c61626364656667680800",
            b"abcdefghabcdefghabcdefgh",
        );
        decode_case("0c0000008061626364656667680800", b"abcdefghabcd");
        decode_case("0a000000336162630300", b"abcabcabca");
        decode_case(
            "14000000f0056162636465666768696a6b6c6d6e6f7071727374",
            b"abcdefghijklmnopqrst",
        );
        decode_case("180000003f616263030002", b"abcabcabcabcabcabcabcabc");

        let snapshot = telemetry::snapshot();
        assert_eq!(
            snapshot.counter(tags::METRIC_COMPRESSION_DECODE_TABLE_TOKEN_COUNT),
            Some(8)
        );
        assert_eq!(
            snapshot.counter(tags::METRIC_COMPRESSION_DECODE_LENGTH_EXTENSION_COUNT),
            Some(2)
        );
        assert_eq!(
            snapshot.counter(tags::METRIC_COMPRESSION_DECODE_COPY_RLE_COUNT),
            Some(1)
        );
        assert_eq!(
            snapshot.counter(tags::METRIC_COMPRESSION_DECODE_COPY_REPEAT2_COUNT),
            Some(1)
        );
        assert_eq!(
            snapshot.counter(tags::METRIC_COMPRESSION_DECODE_COPY_REPEAT4_COUNT),
            Some(1)
        );
        assert_eq!(
            snapshot.counter(tags::METRIC_COMPRESSION_DECODE_COPY_REPEAT8_COUNT),
            Some(1)
        );
        assert_eq!(
            snapshot.counter(tags::METRIC_COMPRESSION_DECODE_COPY_NON_OVERLAP_COUNT),
            Some(1)
        );
        assert_eq!(
            snapshot.counter(tags::METRIC_COMPRESSION_DECODE_COPY_OVERLAP_COUNT),
            Some(2)
        );

        let literal_hist = snapshot
            .histogram(tags::METRIC_COMPRESSION_DECODE_LITERAL_BYTES)
            .expect("literal histogram missing");
        assert_eq!(literal_hist.count, 8);

        let match_hist = snapshot
            .histogram(tags::METRIC_COMPRESSION_DECODE_MATCH_BYTES)
            .expect("match histogram missing");
        assert_eq!(match_hist.count, 7);
    }
}

#[cfg(not(feature = "telemetry"))]
mod telemetry_disabled_tests {
    use oxide_core::compression::lz4;
    use oxide_core::telemetry;
    use oxide_core::telemetry::tags;

    #[test]
    fn lz4_decode_fast_path_metrics_remain_noop_without_feature() {
        telemetry::reset();

        let decoded = lz4::reverse(&[12, 0, 0, 0, 0x17, b'a', 1, 0]).expect("decode succeeds");
        assert_eq!(decoded, vec![b'a'; 12]);

        let snapshot = telemetry::snapshot();
        assert!(
            snapshot
                .counter(tags::METRIC_COMPRESSION_DECODE_TABLE_TOKEN_COUNT)
                .is_none()
        );
        assert!(
            snapshot
                .histogram(tags::METRIC_COMPRESSION_DECODE_MATCH_BYTES)
                .is_none()
        );
    }
}
