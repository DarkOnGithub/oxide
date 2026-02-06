#[cfg(feature = "profiling")]
mod profile_tag_stack_tests {
    use std::sync::Mutex;

    use oxide_core::telemetry::profile;
    use oxide_core::telemetry::tags;

    static PROFILE_TAG_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn supports_enabling_multiple_tags_together() {
        let _guard = PROFILE_TAG_MUTEX.lock().expect("profile tag lock poisoned");

        profile::set_enabled_tags(&[tags::TAG_MMAP, tags::TAG_BUFFER]);

        assert!(profile::is_tag_stack_enabled(&[
            tags::TAG_SYSTEM,
            tags::TAG_MMAP
        ]));
        assert!(profile::is_tag_stack_enabled(&[
            tags::TAG_SYSTEM,
            tags::TAG_BUFFER
        ]));
        assert!(!profile::is_tag_stack_enabled(&[
            tags::TAG_SYSTEM,
            tags::TAG_FORMAT
        ]));

        profile::enable_all_tags();
    }

    #[test]
    fn system_tag_can_enable_shared_events() {
        let _guard = PROFILE_TAG_MUTEX.lock().expect("profile tag lock poisoned");

        profile::set_enabled_tags(&[tags::TAG_SYSTEM]);

        assert!(profile::is_tag_stack_enabled(&[
            tags::TAG_SYSTEM,
            tags::TAG_MMAP
        ]));
        assert!(profile::is_tag_stack_enabled(&[
            tags::TAG_SYSTEM,
            tags::TAG_FORMAT
        ]));
        assert!(profile::is_tag_stack_enabled(&[
            tags::TAG_SYSTEM,
            tags::TAG_BUFFER
        ]));

        profile::enable_all_tags();
    }
}
