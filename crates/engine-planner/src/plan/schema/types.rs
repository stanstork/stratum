use serde::Serialize;

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangeType {
    CreateTable,
    DropTable,
    AddColumn,
    DropColumn,
    ModifyColumn,
    RenameColumn,
    AddIndex,
    DropIndex,
    AddConstraint,
    DropConstraint,
    CreateEnum,
    AlterEnum,
}

impl SchemaChangeType {
    /// Returns symbol for display: "+" for additions, "~" for modifications, "-" for deletions
    pub fn symbol(&self) -> &'static str {
        match self {
            SchemaChangeType::CreateTable
            | SchemaChangeType::AddColumn
            | SchemaChangeType::AddIndex
            | SchemaChangeType::AddConstraint
            | SchemaChangeType::CreateEnum => "+",

            SchemaChangeType::ModifyColumn
            | SchemaChangeType::RenameColumn
            | SchemaChangeType::AlterEnum => "~",

            SchemaChangeType::DropTable
            | SchemaChangeType::DropColumn
            | SchemaChangeType::DropIndex
            | SchemaChangeType::DropConstraint => "-",
        }
    }
}
