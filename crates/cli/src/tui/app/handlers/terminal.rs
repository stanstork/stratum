use crate::tui::app::{
    command::MigrationCommand,
    handlers::events::TerminalEvent,
    state::{AppState, View},
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// Result of handling a terminal event
#[derive(Debug, PartialEq)]
pub enum TerminalAction {
    None,
    Quit,
    QuitConfirm,
    SendCommand(MigrationCommand),
}

/// Handle terminal events
pub fn handle_terminal_event(
    event: TerminalEvent,
    app_state: &AppState,
    current_view: &mut View,
    selected_pipeline: &mut usize,
    pipeline_count: usize,
) -> TerminalAction {
    match event {
        TerminalEvent::Key(key) => handle_key_event(
            key,
            app_state,
            current_view,
            selected_pipeline,
            pipeline_count,
        ),
        _ => TerminalAction::None,
    }
}

/// Handle keyboard input
fn handle_key_event(
    key: KeyEvent,
    app_state: &AppState,
    current_view: &mut View,
    selected_pipeline: &mut usize,
    pipeline_count: usize,
) -> TerminalAction {
    use KeyCode::*;

    // Quit (Ctrl+C or 'q')
    if is_ctrl_c(key) || matches!(key.code, Char('q')) {
        return handle_quit_request(app_state);
    }

    match key.code {
        // View switching
        Tab => {
            *current_view = current_view.next();
            TerminalAction::None
        }
        Char('1') => {
            *current_view = View::Overview;
            TerminalAction::None
        }
        Char('2') => {
            *current_view = View::PipelineDetail;
            TerminalAction::None
        }
        Char('3') => {
            *current_view = View::Errors;
            TerminalAction::None
        }
        Char('4') | Char('?') => {
            *current_view = View::Help;
            TerminalAction::None
        }

        // Navigation
        Up => {
            move_selection(selected_pipeline, -1, pipeline_count);
            TerminalAction::None
        }
        Down => {
            move_selection(selected_pipeline, 1, pipeline_count);
            TerminalAction::None
        }
        Enter => {
            *current_view = View::PipelineDetail;
            TerminalAction::None
        }
        Esc => {
            *current_view = View::Overview;
            TerminalAction::None
        }

        // Control commands (all pipelines)
        Char('p') => TerminalAction::SendCommand(MigrationCommand::PauseAll),
        Char('r') => TerminalAction::SendCommand(MigrationCommand::ResumeAll),
        Char('C') => TerminalAction::SendCommand(MigrationCommand::CancelAll),

        // Control commands (selected pipeline) - uppercase letters
        Char('P') => {
            TerminalAction::SendCommand(MigrationCommand::PausePipeline("selected".to_string()))
        }
        Char('R') => {
            TerminalAction::SendCommand(MigrationCommand::ResumePipeline("selected".to_string()))
        }
        Char('c') => {
            TerminalAction::SendCommand(MigrationCommand::CancelPipeline("selected".to_string()))
        }

        _ => TerminalAction::None,
    }
}

fn handle_quit_request(app_state: &AppState) -> TerminalAction {
    if app_state.is_running() {
        TerminalAction::QuitConfirm
    } else {
        TerminalAction::Quit
    }
}

fn move_selection(selected: &mut usize, delta: i32, count: usize) {
    if count == 0 {
        return;
    }

    let current = *selected as i32;
    let max = count as i32 - 1;
    let next = (current + delta).clamp(0, max) as usize;
    *selected = next;
}

fn is_ctrl_c(key: KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quit_when_running() {
        let state = AppState::Running;
        let mut view = View::Overview;
        let mut selected = 0;
        let key = KeyEvent::from(KeyCode::Char('q'));

        let action = handle_key_event(key, &state, &mut view, &mut selected, 5);
        assert_eq!(action, TerminalAction::QuitConfirm);
    }

    #[test]
    fn test_quit_when_completed() {
        let state = AppState::Completed;
        let mut view = View::Overview;
        let mut selected = 0;
        let key = KeyEvent::from(KeyCode::Char('q'));

        let action = handle_key_event(key, &state, &mut view, &mut selected, 5);
        assert_eq!(action, TerminalAction::Quit);
    }

    #[test]
    fn test_view_cycling() {
        let state = AppState::Running;
        let mut view = View::Overview;
        let mut selected = 0;
        let key = KeyEvent::from(KeyCode::Tab);

        handle_key_event(key, &state, &mut view, &mut selected, 5);
        assert_eq!(view, View::PipelineDetail);

        handle_key_event(key, &state, &mut view, &mut selected, 5);
        assert_eq!(view, View::Errors);

        handle_key_event(key, &state, &mut view, &mut selected, 5);
        assert_eq!(view, View::Help);

        handle_key_event(key, &state, &mut view, &mut selected, 5);
        assert_eq!(view, View::Overview);
    }

    #[test]
    fn test_move_selection_down() {
        let mut selected = 0;
        move_selection(&mut selected, 1, 5);
        assert_eq!(selected, 1);

        move_selection(&mut selected, 1, 5);
        assert_eq!(selected, 2);
    }

    #[test]
    fn test_move_selection_up() {
        let mut selected = 2;
        move_selection(&mut selected, -1, 5);
        assert_eq!(selected, 1);

        move_selection(&mut selected, -1, 5);
        assert_eq!(selected, 0);
    }

    #[test]
    fn test_move_selection_bounds() {
        let mut selected = 0;
        move_selection(&mut selected, -1, 5);
        assert_eq!(selected, 0); // Can't go below 0

        let mut selected = 4;
        move_selection(&mut selected, 1, 5);
        assert_eq!(selected, 4); // Can't go above max
    }

    #[test]
    fn test_ctrl_c_detection() {
        let key = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert!(is_ctrl_c(key));

        let key = KeyEvent::from(KeyCode::Char('c'));
        assert!(!is_ctrl_c(key));
    }
}
