use std::{cell::RefCell, sync::Once};

thread_local! {
    /// Last captured panic message. Read by generated entry points after
    /// `catch_unwind` returns Err.
    pub(crate) static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
}

static PANIC_HOOK: Once = Once::new();

/// Install the panic hook exactly once. Called from generated `__stratum_initialize`.
#[doc(hidden)]
pub fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        std::panic::set_hook(Box::new(|info| {
            let msg = format!("{}", info);
            LAST_PANIC.with(|cell| *cell.borrow_mut() = Some(msg.clone()));
            // Also forward to host log so it's visible in --log-file.
            crate::host::log::log_error(&msg);
        }));
    });
}

#[doc(hidden)]
pub fn take_panic() -> Option<String> {
    LAST_PANIC.with(|cell| cell.borrow_mut().take())
}
