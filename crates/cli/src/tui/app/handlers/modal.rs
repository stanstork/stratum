use crate::tui::{
    app::{command::MigrationCommand, state::View},
    ui::widgets::modal::ModalState,
};
use crossterm::event::{KeyCode, KeyEvent};

/// Handle key events when a modal is active
pub fn handle_modal_key(
    modal_state: &mut ModalState,
    current_view: &mut View,
    key: KeyEvent,
) -> ModalAction {
    match modal_state.clone() {
        ModalState::QuitConfirmation => handle_quit_confirmation(modal_state, key),
        ModalState::MigrationCompleted { .. } => {
            handle_migration_completed(modal_state, current_view, key)
        }
        ModalState::MigrationFailed { pipeline_name, .. } => {
            handle_migration_failed(modal_state, current_view, key, pipeline_name)
        }
        ModalState::None => ModalAction::None,
    }
}

/// Action to take after handling modal input
#[derive(Debug, PartialEq)]
pub enum ModalAction {
    None,
    Quit,
    SendCommand(MigrationCommand),
}

fn handle_quit_confirmation(modal_state: &mut ModalState, key: KeyEvent) -> ModalAction {
    use KeyCode::*;

    match key.code {
        Char('y') | Char('Y') => ModalAction::Quit,
        Char('n') | Char('N') | Esc => {
            *modal_state = ModalState::None;
            ModalAction::None
        }
        _ => ModalAction::None,
    }
}

fn handle_migration_completed(
    modal_state: &mut ModalState,
    current_view: &mut View,
    key: KeyEvent,
) -> ModalAction {
    use KeyCode::*;

    match key.code {
        Char('q') | Char('Q') => ModalAction::Quit,
        Char('e') | Char('E') => {
            *modal_state = ModalState::None;
            *current_view = View::Errors;
            ModalAction::None
        }
        Esc => {
            *modal_state = ModalState::None;
            ModalAction::None
        }
        _ => ModalAction::None,
    }
}

fn handle_migration_failed(
    modal_state: &mut ModalState,
    current_view: &mut View,
    key: KeyEvent,
    pipeline_name: String,
) -> ModalAction {
    use KeyCode::*;

    match key.code {
        Char('q') | Char('Q') => ModalAction::Quit,
        Char('e') | Char('E') => {
            *modal_state = ModalState::None;
            *current_view = View::Errors;
            ModalAction::None
        }
        Char('r') | Char('R') => {
            *modal_state = ModalState::None;
            ModalAction::SendCommand(MigrationCommand::RetryPipeline(pipeline_name))
        }
        Esc => {
            *modal_state = ModalState::None;
            ModalAction::None
        }
        _ => ModalAction::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quit_confirmation_yes() {
        let mut modal = ModalState::QuitConfirmation;
        let key = KeyEvent::from(KeyCode::Char('y'));

        let action = handle_quit_confirmation(&mut modal, key);
        assert_eq!(action, ModalAction::Quit);
    }

    #[test]
    fn test_quit_confirmation_no() {
        let mut modal = ModalState::QuitConfirmation;
        let key = KeyEvent::from(KeyCode::Char('n'));

        let action = handle_quit_confirmation(&mut modal, key);
        assert_eq!(action, ModalAction::None);
        assert_eq!(modal, ModalState::None);
    }

    #[test]
    fn test_migration_completed_quit() {
        let mut modal = ModalState::MigrationCompleted {
            total_rows: 1000,
            duration: std::time::Duration::from_secs(10),
            avg_throughput: 100.0,
            warnings: 0,
            errors: 0,
            skipped: 0,
        };
        let mut view = View::Overview;
        let key = KeyEvent::from(KeyCode::Char('q'));

        let action = handle_migration_completed(&mut modal, &mut view, key);
        assert_eq!(action, ModalAction::Quit);
    }

    #[test]
    fn test_migration_failed_retry() {
        let mut modal = ModalState::MigrationFailed {
            pipeline_name: "test_pipeline".to_string(),
            error_message: "Test error".to_string(),
            error_count: 5,
        };
        let mut view = View::Overview;
        let key = KeyEvent::from(KeyCode::Char('r'));

        let action =
            handle_migration_failed(&mut modal, &mut view, key, "test_pipeline".to_string());

        match action {
            ModalAction::SendCommand(MigrationCommand::RetryPipeline(name)) => {
                assert_eq!(name, "test_pipeline");
            }
            _ => panic!("Expected SendCommand action"),
        }
        assert_eq!(modal, ModalState::None);
    }
}
