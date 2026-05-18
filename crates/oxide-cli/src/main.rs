mod cli;
mod commands;
mod presets;
mod progress;
mod report;
mod tree;
mod ui;

use clap::Parser;
use cli::{Cli, Commands};
use ui::print_startup_banner;

type AppResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn main() {
    tracing_subscriber::fmt::init();
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> AppResult {
    let cli = Cli::parse();

    if cli.gui || matches!(cli.command, None | Some(Commands::Gui)) {
        return commands::gui();
    }

    print_startup_banner();

    match cli.command.expect("command presence checked above") {
        Commands::Gui => unreachable!("gui command handled before CLI banner"),
        Commands::Archive(args) => commands::archive(*args)?,
        Commands::Extract(args) => commands::extract(args)?,
        Commands::Tree(args) => commands::tree(args)?,
        Commands::Encrypt(args) => commands::encrypt(args)?,
        Commands::Decrypt(args) => commands::decrypt(args)?,
        Commands::Protect(args) => commands::protect(args)?,
        Commands::Repair(args) => commands::repair(args)?,
    }

    Ok(())
}
