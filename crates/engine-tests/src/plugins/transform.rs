//! MySQL -> PostgreSQL with a Rust transform plugin.

#[cfg(test)]
mod tests {
    use crate::{
        plugins::fixture,
        reset_postgres_schema,
        utils::{
            DbType, assert_column_exists, assert_table_exists, get_cell_as_f64, get_column_names,
            get_pg_column_type, get_row_count, run_smql,
        },
    };
    use tracing_test::traced_test;

    /// Build an SMQL doc for a `film -> <dest>` pipeline whose `select` block is
    /// supplied by the caller. The `test_transform` plugin is declared as `adder`.
    fn smql(dest_table: &str, select_block: &str, extra_settings: &str) -> String {
        format!(
            r#"
            plugin "adder" {{ path = "{plugin}" }}

            connection "mysql_source" {{
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }}
            connection "pg_destination" {{
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }}

            pipeline "migrate_film_priced" {{
                from {{ connection = connection.mysql_source  table = "film" }}
                to   {{ connection = connection.pg_destination table = "{dest}" }}

                select {{
                    {select}
                }}

                settings {{
                    create_missing_tables = true
                    {extra}
                }}
            }}
            "#,
            plugin = fixture("test_transform.wasm"),
            dest = dest_table,
            select = select_block,
            extra = extra_settings,
        )
    }

    /// The plugin output column is created in the new table and every source row is migrated.
    #[traced_test]
    #[tokio::test]
    async fn transform_creates_output_column_and_copies_all_rows() {
        reset_postgres_schema().await;

        let doc = smql(
            "films_priced",
            r#"film_id    = film.film_id
               total_cost = plugin.adder({ a: film.rental_rate, b: film.replacement_cost })"#,
            r#"copy_columns = "MAP_ONLY""#,
        );

        run_smql(&doc, false).await.expect("migration succeeds");

        assert_table_exists("films_priced", true).await;
        assert_column_exists("films_priced", "total_cost", true).await;

        let src = get_row_count("film", "sakila", DbType::MySql).await;
        let dst = get_row_count("films_priced", "sakila", DbType::Postgres).await;
        assert_eq!(src, dst, "all film rows should be migrated");

        // MAP_ONLY: only the two selected columns exist.
        let cols = get_column_names(DbType::Postgres, "sakila", "films_priced")
            .await
            .unwrap();
        assert_eq!(
            cols.len(),
            2,
            "expected only film_id + total_cost, got {cols:?}"
        );
    }

    /// The plugin output column is created as `double precision` (its `f64`
    /// canonical type), not inferred from any source column.
    #[traced_test]
    #[tokio::test]
    async fn transform_output_column_is_double_precision() {
        reset_postgres_schema().await;

        let doc = smql(
            "films_priced",
            r#"film_id    = film.film_id
               total_cost = plugin.adder({ a: film.rental_rate, b: film.replacement_cost })"#,
            r#"copy_columns = "MAP_ONLY""#,
        );

        run_smql(&doc, false).await.expect("migration succeeds");

        let ty = get_pg_column_type("films_priced", "total_cost").await;
        assert_eq!(ty, "double precision", "plugin f64 output column type");
    }

    /// The transform actually computes `a + b`, and DECIMAL source columns are
    /// coerced to `f64` before the call (regression: decimal inputs used to fail).
    #[traced_test]
    #[tokio::test]
    async fn transform_computes_sum_with_decimal_inputs() {
        reset_postgres_schema().await;

        let doc = smql(
            "films_priced",
            r#"film_id    = film.film_id
               total_cost = plugin.adder({ a: film.rental_rate, b: film.replacement_cost })"#,
            r#"copy_columns = "MAP_ONLY""#,
        );

        run_smql(&doc, false).await.expect("migration succeeds");

        for film_id in [1, 2, 3, 500, 1000] {
            let expected = get_cell_as_f64(
                &format!(
                    "SELECT CAST(rental_rate + replacement_cost AS DOUBLE) AS s \
                     FROM film WHERE film_id = {film_id}"
                ),
                "sakila",
                DbType::MySql,
                "s",
            )
            .await;

            let actual = get_cell_as_f64(
                &format!("SELECT total_cost FROM films_priced WHERE film_id = {film_id}"),
                "sakila",
                DbType::Postgres,
                "total_cost",
            )
            .await;

            assert!(
                (expected - actual).abs() < 1e-6,
                "film {film_id}: expected total_cost {expected}, got {actual}"
            );
        }
    }

    /// A plugin output column coexists with directly-mapped source columns.
    #[traced_test]
    #[tokio::test]
    async fn transform_alongside_mapped_source_columns() {
        reset_postgres_schema().await;

        let doc = smql(
            "films_priced",
            r#"film_id          = film.film_id
               title            = film.title
               rental_rate      = film.rental_rate
               replacement_cost = film.replacement_cost
               total_cost       = plugin.adder({ a: film.rental_rate, b: film.replacement_cost })"#,
            r#"copy_columns = "MAP_ONLY""#,
        );

        run_smql(&doc, false).await.expect("migration succeeds");

        for col in [
            "film_id",
            "title",
            "rental_rate",
            "replacement_cost",
            "total_cost",
        ] {
            assert_column_exists("films_priced", col, true).await;
        }

        let src = get_row_count("film", "sakila", DbType::MySql).await;
        let dst = get_row_count("films_priced", "sakila", DbType::Postgres).await;
        assert_eq!(src, dst);
    }

    /// When the plugin output column shadows a source column of a different type,
    /// the destination column is (re)typed to the plugin's output type and the
    /// computed value wins (the source column's values are discarded).
    #[traced_test]
    #[tokio::test]
    async fn transform_output_shadowing_source_column_retypes_it() {
        reset_postgres_schema().await;

        // `length` is SMALLINT in Sakila; the plugin output is f64.
        let doc = smql(
            "films_shadow",
            r#"film_id = film.film_id
               length  = plugin.adder({ a: film.rental_rate, b: film.replacement_cost })"#,
            r#"copy_columns = "MAP_ONLY""#,
        );

        run_smql(&doc, false).await.expect("migration succeeds");

        let ty = get_pg_column_type("films_shadow", "length").await;
        assert_eq!(
            ty, "double precision",
            "shadowed column should be retyped to f64"
        );

        let expected = get_cell_as_f64(
            "SELECT CAST(rental_rate + replacement_cost AS DOUBLE) AS s FROM film WHERE film_id = 1",
            "sakila",
            DbType::MySql,
            "s",
        )
        .await;
        let actual = get_cell_as_f64(
            "SELECT length FROM films_shadow WHERE film_id = 1",
            "sakila",
            DbType::Postgres,
            "length",
        )
        .await;
        assert!(
            (expected - actual).abs() < 1e-6,
            "shadowed column should hold the computed value, got {actual} vs {expected}"
        );
    }
}
