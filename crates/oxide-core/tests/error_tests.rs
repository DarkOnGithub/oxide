use oxide_core::OxideError;

#[test]
fn converts_io_error() {
    let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
    let oxide_error: OxideError = io_error.into();

    match oxide_error {
        OxideError::Io(err) => assert_eq!(err.kind(), std::io::ErrorKind::NotFound),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn converts_anyhow_error() {
    let anyhow_error = anyhow::anyhow!("boom");
    let oxide_error: OxideError = anyhow_error.into();

    match oxide_error {
        OxideError::Other(err) => assert_eq!(err.to_string(), "boom"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn attaches_context() {
    let err = OxideError::InvalidFormat("bad footer").with_context("while parsing block");

    match err {
        OxideError::Context { context, source } => {
            assert_eq!(context, "while parsing block");
            assert!(matches!(*source, OxideError::InvalidFormat("bad footer")));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
