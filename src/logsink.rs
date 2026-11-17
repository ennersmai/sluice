//! Process-wide log output: every line goes to stdout (request lines) or
//! stderr (warnings) and, when `log_file` is configured, also to that file.
//! One std Mutex around a buffered writer, flushed per line; never held across
//! an await.

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Mutex, OnceLock, PoisonError};

static FILE: OnceLock<Mutex<Option<BufWriter<std::fs::File>>>> = OnceLock::new();

/// Open `path` for appending. Called once at startup; later calls are ignored.
pub fn init(path: &Path) -> Result<(), String> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("cannot open log_file {}: {e}", path.display()))?;
    let _ = FILE.set(Mutex::new(Some(BufWriter::new(file))));
    Ok(())
}

fn to_file(line: &str) {
    let Some(m) = FILE.get() else { return };
    let mut guard = m.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(w) = guard.as_mut() else { return };
    if writeln!(w, "{line}").and_then(|()| w.flush()).is_err() {
        // Warn once, then stop writing to the file.
        *guard = None;
        eprintln!("warn: cannot write to log_file, file logging stopped");
    }
}

/// A request line: stdout and the file.
pub fn out(line: &str) {
    println!("{line}");
    to_file(line);
}

/// A warning or error line: stderr and the file.
pub fn err(line: &str) {
    eprintln!("{line}");
    to_file(line);
}

/// `eprintln!` that also reaches the log file.
#[macro_export]
macro_rules! elog {
    ($($t:tt)*) => {
        $crate::logsink::err(&format!($($t)*))
    };
}
