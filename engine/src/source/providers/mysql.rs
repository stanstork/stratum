use crate::{record::Record, source::data_source::DbDataSource};
use async_trait::async_trait;
use common::computed;
use common::mapping::{EntityFieldsMap, NameMap};
use mysql::mysql::MySqlAdapter;
use smql::statements::expr::Expression;
use sql_adapter::adapter::SqlAdapter;
use sql_adapter::join::{self, Join, JoinClause};
use sql_adapter::metadata::table;
use sql_adapter::{metadata::table::TableMetadata, requests::FetchRowsRequest};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::field;

pub struct MySqlDataSource {
    metadata: HashMap<String, TableMetadata>,
    entity_name_map: NameMap,
    entity_field_map: EntityFieldsMap,
    adapter: MySqlAdapter,
}

impl MySqlDataSource {
    pub fn new(
        adapter: MySqlAdapter,
        entity_name_map: NameMap,
        entity_field_map: EntityFieldsMap,
    ) -> Self {
        Self {
            metadata: HashMap::new(),
            entity_name_map,
            entity_field_map,
            adapter,
        }
    }

    pub fn set_entity_name_map(&mut self, entity_name_map: NameMap) {
        self.entity_name_map = entity_name_map;
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    async fn fetch_data(
        &self,
        batch_size: usize,
        joins: Vec<Join>,
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
        let mut records = Vec::new();
        let mut processed_tables = HashSet::new();

        for table in self.metadata.keys() {
            let grouped_fields = self.get_metadata(table).select_fields();
            for (tbl, fields) in grouped_fields {
                if !processed_tables.insert(tbl.clone()) {
                    continue;
                }

                let related_joins = Join::collect_related_joins(tbl.clone(), &joins);

                let mut joined_fields = Vec::new();
                for j in &related_joins {
                    let fields = j.source_metadata.select_fields();
                    let mut fields = fields
                        .get(&j.source_metadata.name)
                        .map(|f| f.clone())
                        .unwrap_or_default();

                    for field1 in fields.iter_mut() {
                        field1.table = j.join_clause.left.alias.clone();

                        let get_computed = self
                            .entity_field_map
                            .get_computed(&self.entity_name_map.resolve(&tbl));

                        if let Some(c_field) = get_computed {
                            for f in c_field.iter() {
                                match &f.expression {
                                    Expression::Lookup { table, key, field } => {
                                        if field1.table.eq_ignore_ascii_case(&table)
                                            && field1.column.eq_ignore_ascii_case(&key)
                                        {
                                            field1.alias = Some(f.name.clone());
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }

                        if j.join_clause.fields.contains(&field1.column) {
                            joined_fields.push(field1.clone());
                        }
                    }
                }

                let mut fields = fields.clone();
                fields.extend(joined_fields);

                let mut join_clause = related_joins
                    .iter()
                    .map(|j| j.join_clause.clone())
                    .collect::<Vec<_>>();

                for j in join_clause.iter_mut() {
                    j.right.table = self.entity_name_map.reverse_resolve(&j.right.table);
                    j.right.alias = j.right.table.clone();
                }

                println!("Join clause: {:?}", join_clause);

                let request = FetchRowsRequest::new(
                    tbl.clone(),
                    Some(self.entity_name_map.resolve(&tbl)),
                    Some(tbl.clone()),
                    fields,
                    join_clause,
                    batch_size,
                    offset,
                );

                let rows = self.adapter.fetch_rows(request).await?;

                println!("Rows: {:?}", rows);

                records.extend(rows.into_iter().map(Record::RowData));
            }
        }

        Ok(records)
    }

    fn get_metadata(&self, table: &str) -> &TableMetadata {
        self.metadata
            .get(table)
            .unwrap_or_else(|| panic!("Metadata for table {} not found", table))
    }

    fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>) {
        self.metadata = metadata;
    }

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)> {
        Arc::new(self.adapter.clone())
    }
}
