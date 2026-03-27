//! Terminal launch commands.

use std::path::Path;
use std::process::Command;

fn validate_path(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Err(format!("Path does not exist: {}", path.display()));
    }

    if !path.is_dir() {
        return Err(format!("Path is not a folder: {}", path.display()));
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn escape_applescript_string(value: &str) -> String {
    value.replace('\\', r"\\").replace('"', r#"\""#)
}

#[cfg(target_os = "macos")]
fn shell_escape_single_quoted(path: &Path) -> String {
    let value = path.to_string_lossy();
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(target_os = "macos")]
fn open_terminal(path: &Path) -> Result<(), String> {
    let shell_path = shell_escape_single_quoted(path);
    let shell_command = format!("cd {} && clear", shell_path);
    let apple_script = format!(
        "tell application \"Terminal\" to do script \"{}\"\ntell application \"Terminal\" to activate",
        escape_applescript_string(&shell_command)
    );

    Command::new("osascript")
        .arg("-e")
        .arg(apple_script)
        .spawn()
        .map_err(|e| format!("Failed to open Terminal.app: {e}"))?;

    Ok(())
}

#[cfg(target_os = "windows")]
fn open_terminal(path: &Path) -> Result<(), String> {
    Command::new("cmd")
        .current_dir(path)
        .args(["/K"])
        .spawn()
        .map_err(|e| format!("Failed to open terminal: {e}"))?;

    Ok(())
}

#[cfg(target_os = "linux")]
fn open_terminal(path: &Path) -> Result<(), String> {
    const CANDIDATES: &[&str] = &[
        "x-terminal-emulator",
        "gnome-terminal",
        "konsole",
        "xfce4-terminal",
        "kitty",
        "alacritty",
        "wezterm",
        "xterm",
    ];

    let mut last_error = None;

    for candidate in CANDIDATES {
        match Command::new(candidate).current_dir(path).spawn() {
            Ok(_) => return Ok(()),
            Err(error) => last_error = Some((candidate, error)),
        }
    }

    if let Some((candidate, error)) = last_error {
        Err(format!(
            "Failed to open a terminal (last attempt: {candidate}): {error}"
        ))
    } else {
        Err("No supported terminal emulator found".to_string())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
fn open_terminal(_path: &Path) -> Result<(), String> {
    Err("Terminal launching is not supported on this platform".to_string())
}

/// Opens a terminal in the specified folder.
#[tauri::command]
#[specta::specta]
pub async fn open_terminal_in_current_folder(path: String) -> Result<(), String> {
    let folder = Path::new(&path);
    validate_path(folder)?;

    log::info!("Opening terminal in folder: {path}");
    open_terminal(folder)
}
