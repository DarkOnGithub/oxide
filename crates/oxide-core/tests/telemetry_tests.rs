#[cfg(feature = "telemetry")]
mod telemetry_enabled_tests {
    use std::io::Write;
    use std::sync::Mutex;
    use std::time::Duration;

    use oxide_core::format::FormatDetector;
    use oxide_core::telemetry;
    use oxide_core::telemetry::tags;
    use oxide_core::{
        BufferPool, DefaultWorkerTelemetry, InputScanner, MmapInput, WorkerTelemetry,
    };
    use tempfile::NamedTempFile;

    static TELEMETRY_TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn records_metrics_for_hotspots() -> Result<(), Box<dyn std::error::Error>> {
        let _guard = TELEMETRY_TEST_MUTEX
            .lock()
            .expect("telemetry test lock poisoned");

        telemetry::reset();

        let mut file = NamedTempFile::new()?;
        file.write_all(b"hello telemetry")?;
        file.flush()?;

        let mmap = MmapInput::open(file.path())?;
        let _ = mmap.slice(0, 5)?;
        let _ = mmap.as_bytes()?;

        let _ = FormatDetector::detect(b"this is plain text");
        let scanner = InputScanner::new(8);
        let batches = scanner.scan_file(file.path())?;
        assert!(!batches.is_empty());

        let pool = BufferPool::new(64, 1);
        {
            let _first = pool.acquire();
        }
        {
            let _second = pool.acquire();
        }

        let snapshot = telemetry::snapshot();
        assert!(snapshot.counter(tags::METRIC_MMAP_OPEN_COUNT).unwrap_or(0) >= 2);
        assert!(snapshot.counter(tags::METRIC_MMAP_SLICE_COUNT).unwrap_or(0) >= 2);
        assert!(
            snapshot
                .counter(tags::METRIC_FORMAT_DETECT_COUNT)
                .unwrap_or(0)
                >= 2
        );
        assert_eq!(
            snapshot.counter(tags::METRIC_BUFFER_ACQUIRE_CREATED_COUNT),
            Some(1)
        );
        assert_eq!(
            snapshot.counter(tags::METRIC_BUFFER_ACQUIRE_RECYCLED_COUNT),
            Some(1)
        );
        assert_eq!(snapshot.counter(tags::METRIC_SCANNER_SCAN_COUNT), Some(1));
        assert_eq!(
            snapshot.counter(tags::METRIC_SCANNER_MODE_TEXT_COUNT),
            Some(1)
        );

        let open_hist = snapshot
            .histogram(tags::METRIC_MMAP_OPEN_LATENCY_US)
            .expect("mmap open histogram missing");
        assert!(open_hist.count >= 2);

        let detect_hist = snapshot
            .histogram(tags::METRIC_FORMAT_DETECT_LATENCY_US)
            .expect("format detect histogram missing");
        assert!(detect_hist.count >= 2);
        let scanner_hist = snapshot
            .histogram(tags::METRIC_SCANNER_SCAN_LATENCY_US)
            .expect("scanner scan histogram missing");
        assert_eq!(scanner_hist.count, 1);

        assert!(
            snapshot
                .gauge(tags::METRIC_MEMORY_POOL_ESTIMATED_BYTES)
                .unwrap_or(0)
                > 0
        );

        Ok(())
    }

    #[test]
    fn memory_sampling_updates_gauges() {
        let _guard = TELEMETRY_TEST_MUTEX
            .lock()
            .expect("telemetry test lock poisoned");

        telemetry::reset();

        let sample = telemetry::sample_process_memory();
        let snapshot = telemetry::snapshot();

        if let Some(rss) = sample.rss_bytes {
            assert_eq!(
                snapshot.gauge(tags::METRIC_MEMORY_PROCESS_RSS_BYTES),
                Some(rss)
            );
        }

        if let Some(virtual_bytes) = sample.virtual_bytes {
            assert_eq!(
                snapshot.gauge(tags::METRIC_MEMORY_PROCESS_VIRTUAL_BYTES),
                Some(virtual_bytes)
            );
        }
    }

    #[test]
    fn worker_scaffold_emits_metrics() {
        let _guard = TELEMETRY_TEST_MUTEX
            .lock()
            .expect("telemetry test lock poisoned");

        telemetry::reset();

        let worker_telemetry = DefaultWorkerTelemetry;
        worker_telemetry.on_queue_depth(0, 7);
        worker_telemetry.on_task_started(0, "compress");
        worker_telemetry.on_task_finished(0, "compress", Duration::from_micros(120));
        worker_telemetry.on_task_started(0, "compress");
        worker_telemetry.on_task_failed(0, "compress", Duration::from_micros(75));

        let snapshot = telemetry::snapshot();
        assert_eq!(snapshot.gauge(tags::METRIC_WORKER_QUEUE_DEPTH), Some(7));
        assert_eq!(snapshot.gauge(tags::METRIC_WORKER_ACTIVE_COUNT), Some(0));
        assert_eq!(snapshot.counter(tags::METRIC_WORKER_TASK_COUNT), Some(2));

        let task_hist = snapshot
            .histogram(tags::METRIC_WORKER_TASK_LATENCY_US)
            .expect("worker task histogram missing");
        assert_eq!(task_hist.count, 2);
        assert!(task_hist.max >= task_hist.min);
    }
}

#[cfg(not(feature = "telemetry"))]
mod telemetry_disabled_tests {
    use std::time::Duration;

    use oxide_core::telemetry;
    use oxide_core::telemetry::tags;
    use oxide_core::{DefaultWorkerTelemetry, WorkerTelemetry};

    #[test]
    fn telemetry_api_is_noop_without_feature() {
        telemetry::reset();

        telemetry::increment_counter(tags::METRIC_WORKER_TASK_COUNT, 7, &[]);
        telemetry::record_histogram(tags::METRIC_WORKER_TASK_LATENCY_US, 11, &[]);
        telemetry::set_gauge(tags::METRIC_WORKER_QUEUE_DEPTH, 3, &[]);
        telemetry::add_gauge(tags::METRIC_WORKER_QUEUE_DEPTH, 2, &[]);
        telemetry::sub_gauge_saturating(tags::METRIC_WORKER_QUEUE_DEPTH, 10, &[]);
        let _ = telemetry::sample_process_memory();

        let worker_telemetry = DefaultWorkerTelemetry;
        worker_telemetry.on_queue_depth(0, 7);
        worker_telemetry.on_task_started(0, "compress");
        worker_telemetry.on_task_finished(0, "compress", Duration::from_micros(120));
        worker_telemetry.on_task_failed(0, "compress", Duration::from_micros(75));

        let snapshot = telemetry::snapshot();
        assert!(snapshot.counters.is_empty());
        assert!(snapshot.gauges.is_empty());
        assert!(snapshot.histograms.is_empty());
    }
}
