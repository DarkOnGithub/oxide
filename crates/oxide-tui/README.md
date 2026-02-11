# oxide-tui

Terminal UI for `oxide-core` focused on live telemetry, profiling visibility, and run reports.

## Features

- Archive and extract runs from a single interface.
- Single dashboard layout with:
  - always-visible run configuration/actions
  - detail pane (`Live` / `Profile` / `Logs`) toggled with `m`
- Real-time telemetry rendering:
  - progress
  - read/write/decode throughput values (avg + instant)
  - compression/ratio values
  - live per-worker utilization/tasks/busy/idle
- Profiling stream support via global telemetry sink (`TelemetryEvent::Profile`).
- Advanced profiling aggregates:
  - count
  - total/avg/min/max latency
  - rolling `p50`/`p95`
  - event rate
  - target/op/tag filtering + sortable views
- File/folder browser modal for choosing input/output paths.
- Final report visibility (`ArchiveReport` / `ExtractReport`).

## Key Bindings

- `m` / `M`: cycle detail pane (`Live`, `Profile`, `Logs`)
- `Tab` / `Shift+Tab`: move between config fields
- `Enter`: edit selected text field or toggle enum field
- `Left` / `Right`: cycle mode/compression
- `b`: open file browser for current input/output field
- `e`: set output to ephemeral temp target (auto-deleted after run)
- `n`: set output to null sink target (discard output, no persisted file)
- `r`: run
- `d`: fill default output path from current input + mode
- `c`: clear telemetry/report state
- `p`: cycle profile sort mode
- `t` / `o` / `g`: cycle profile target/op/tag filters
- `w`: cycle worker sort mode
- `+` / `-`: adjust visible rows in Live/Profile detail panes
- `,` / `.`: decrease/increase profile rolling window (seconds)
- `Up` / `Down` / `PageUp` / `PageDown`: scroll logs/profile detail panes
- `q`: quit

Ephemeral output can also be set manually by writing `:ephemeral` (or `:temp`) in the output field.

Null output can be set manually by writing `:null` (or `:devnull`) in the output field.

## Mouse

- Dashboard:
  - click a field to focus/edit/toggle
  - click action buttons (`Run`, `Browse Input`, `Browse Output`, `Ephemeral`, `Null Output`, `Default Output`, `Clear`)
- Profile/Logs detail panes:
  - mouse wheel scrolls visible rows
- Browser modal:
  - click rows to select
  - mouse wheel moves selection
  - click action buttons to apply actions

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

- `src/app.rs`: state machine, key/mouse handling, telemetry aggregation
- `src/job.rs`: background run execution and sink wiring
- `src/browser.rs`: reusable file-system browser model
- `src/ui.rs`: rendering + hit-test layout metadata for mouse actions
- `src/formatters.rs`: parsing and display helpers

To add new telemetry/report panels, extend `App::handle_telemetry_event` and render a new section in `ui.rs`.
