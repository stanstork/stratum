use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

const FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

pub struct Spinner {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Spinner {
    /// Start a spinner with `message`, animating on stderr until dropped.
    /// `color` cyan-tints the frame when true.
    pub fn start(message: impl Into<String>, color: bool) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let message = message.into();

        let handle = std::thread::spawn(move || {
            let mut err = std::io::stderr();
            let mut i = 0usize;
            while !stop_flag.load(Ordering::Relaxed) {
                let frame = FRAMES[i % FRAMES.len()];
                let frame = if color {
                    format!("\x1b[36m{frame}\x1b[0m")
                } else {
                    frame.to_string()
                };
                // \r returns to column 0, \x1b[2K clears the whole line.
                let _ = write!(err, "\r\x1b[2K{frame} {message}");
                let _ = err.flush();
                i += 1;
                std::thread::sleep(Duration::from_millis(90));
            }
            let _ = write!(err, "\r\x1b[2K");
            let _ = err.flush();
        });

        Self {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
