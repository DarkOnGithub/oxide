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
use::crates::oxide-gui::src::main::*;
type AppResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn main() {
    tracing_subscriber::fmt::init();
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> AppResult {
    print_startup_banner();
    let cli = Cli::parse();

    match cli.command {
        Commands::Archive(args) => commands::archive(args)?,
        Commands::Extract(args) => commands::extract(args)?,
        Commands::Tree(args) => commands::tree(args)?,
        _ => print(),
    }

    Ok(())
}
