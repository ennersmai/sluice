//! `sluice` binary: argument parsing, `.env` and config loading, server
//! start and graceful shutdown. All logic lives in the library crate.

use std::path::PathBuf;
use std::process::ExitCode;

use sluice::AppState;
use sluice::config::{Config, DEFAULT_CONFIG_PATH};

const USAGE: &str = "\
Usage: sluice [--config <path>]

Runs the router.

Options:
  --config <path>  Config file to load (default: router.toml)
  -h, --help       Print this help
  -V, --version    Print the version

Secrets are read from the environment and from a .env file in the
current directory, if present.";

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Serve { config: PathBuf },
    Help,
    Version,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut config: Option<PathBuf> = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let value = match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "--config" => match args.next() {
                Some(v) => v,
                None => return Err("--config requires a path".to_string()),
            },
            other => match other.strip_prefix("--config=") {
                Some(v) => v.to_string(),
                None => return Err(format!("unexpected argument '{other}'")),
            },
        };
        if value.is_empty() {
            return Err("--config requires a path".to_string());
        }
        if config.replace(PathBuf::from(value)).is_some() {
            return Err("--config given more than once".to_string());
        }
    }
    Ok(Command::Serve {
        config: config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH)),
    })
}

fn main() -> ExitCode {
    match parse_args(std::env::args().skip(1)) {
        Ok(Command::Help) => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Ok(Command::Version) => {
            println!("sluice {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Ok(Command::Serve { config }) => match run(config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(msg) => {
                eprintln!("sluice: {msg}");
                ExitCode::FAILURE
            }
        },
        Err(msg) => {
            eprintln!("sluice: {msg}\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

/// Load `.env`. A missing file is fine. Parse errors are reported without
/// the offending line, since it may contain a secret.
fn load_dotenv() -> Result<(), String> {
    match dotenvy::dotenv() {
        Ok(_) => Ok(()),
        Err(e) if e.not_found() => Ok(()),
        Err(dotenvy::Error::LineParse(_, index)) => Err(format!(
            "cannot parse .env: syntax error at character {index} of a line (line not shown, it may contain a secret)"
        )),
        Err(e) => Err(format!("cannot load .env: {e}")),
    }
}

fn run(config_path: PathBuf) -> Result<(), String> {
    load_dotenv()?;
    let config =
        Config::load(&config_path).map_err(|e| format!("config {}: {e}", config_path.display()))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start async runtime: {e}"))?;

    let state = AppState::new(config)?;

    runtime.block_on(async move {
        let config = &state.config;
        let bind = config.bind;
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .map_err(|e| format!("cannot listen on {bind}: {e}"))?;
        let local = listener.local_addr().unwrap_or(bind);
        println!(
            "sluice {} listening on http://{local} ({} providers, {} pools)",
            env!("CARGO_PKG_VERSION"),
            config.providers.len(),
            config.pools.len()
        );
        sluice::state::load(&state);
        let saver = tokio::spawn(periodic_save(state.clone()));
        let served = sluice::serve(listener, state.clone(), shutdown_signal()).await;
        saver.abort();
        sluice::state::save_logged(&state);
        served.map_err(|e| format!("server error: {e}"))?;
        println!("sluice stopped");
        Ok(())
    })
}

/// Write the state file every 30 s.
async fn periodic_save(state: std::sync::Arc<AppState>) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
    tick.tick().await; // the first tick completes immediately
    loop {
        tick.tick().await;
        sluice::state::save_logged(&state);
    }
}

/// Resolves on Ctrl+C (and SIGTERM on Unix).
async fn shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    println!("sluice: shutting down");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Command, String> {
        parse_args(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn no_args_serves_default_config() {
        assert_eq!(
            parse(&[]),
            Ok(Command::Serve {
                config: PathBuf::from("router.toml")
            })
        );
    }

    #[test]
    fn config_flag_forms() {
        let want = Ok(Command::Serve {
            config: PathBuf::from("x.toml"),
        });
        assert_eq!(parse(&["--config", "x.toml"]), want);
        assert_eq!(parse(&["--config=x.toml"]), want);
    }

    #[test]
    fn help_and_version() {
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
        assert_eq!(parse(&["-h"]), Ok(Command::Help));
        assert_eq!(parse(&["--version"]), Ok(Command::Version));
        assert_eq!(parse(&["-V"]), Ok(Command::Version));
    }

    #[test]
    fn bad_args_are_rejected() {
        assert!(parse(&["--config"]).is_err());
        assert!(parse(&["--config="]).is_err());
        assert!(parse(&["--config", "a", "--config", "b"]).is_err());
        assert!(parse(&["serve"]).is_err());
        assert!(parse(&["--verbose"]).is_err());
    }
}
