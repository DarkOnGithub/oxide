use std::sync::Mutex;

pub(crate) static TELEMETRY_TEST_MUTEX: Mutex<()> = Mutex::new(());
