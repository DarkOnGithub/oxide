# oxide-tui

Terminal UI for `oxide-core` focused on live telemetry, profiling visibility, and run reports.

## Features

- Archive and extract runs from a single interface.
- Real-time telemetry rendering:
  - progress
  - read/write/decode throughput
  - worker activity snapshot
- Profiling stream support via global telemetry sink (`TelemetryEvent::Profile`).
- File/folder browser modal for choosing input/output paths.
- Final report visibility (`ArchiveReport` / `ExtractReport`).

## Key Bindings

- `Tab` / `Shift+Tab`: move between config fields
- `Enter`: edit selected text field or toggle enum field
- `Left` / `Right`: cycle mode/compression
- `b`: open file browser for input/output field
- `r`: run
- `d`: fill default output path from current input + mode
- `c`: clear telemetry/report state
- `q`: quit

Browser modal:

- `Up` / `Down`: move selection
- `Enter`: open directory or select file
- `s`: select current highlighted entry
- `d`: select current directory
- `Left` / `Backspace`: go to parent directory
- `h`: toggle hidden files
- `r`: refresh listing
- `q` / `Esc`: close

## Extensibility Notes

The crate is intentionally split by responsibility:

- `src/app.rs`: state machine, key handling, telemetry aggregation
- `src/job.rs`: background run execution and sink wiring
- `src/browser.rs`: reusable file-system browser model
- `src/ui.rs`: rendering only
- `src/formatters.rs`: parsing and display helpers

To add new telemetry/report panels, extend `App::handle_telemetry_event` and render a new section in `ui.rs`.
