use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HookImpact {
    SchemaChange {
        operation: String,
        target: String,
    },
    IndexOperation {
        is_concurrent: bool,
        is_destructive: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },
    TriggerOperation {
        action: String,
    },
    Maintenance {
        operation: String,
    },
    DataOperation {
        operation: String,
        estimated_rows: Option<u64>,
        is_bulk: bool,
    },
    Other {
        description: String,
    },
}
