#[cfg(test)]
mod tests {
    use crate::harness::{
        db::Dbms,
        fixtures::{reset_mysql_dest, reset_postgres_schema},
        ppl::dest_connection,
        runner::run_ppl,
    };
    use std::io::Write;
    use tempfile::NamedTempFile;

    const PEOPLE_CSV: &str = "id,first_name,last_name,score,active\n\
         1,Ada,Lovelace,95.5,true\n\
         2,Alan,Turing,99.0,true\n\
         3,Grace,Hopper,88.0,false\n";

    /// Write `contents` to a temp `.csv` file kept alive for the test's duration.
    fn temp_csv(contents: &str) -> NamedTempFile {
        let mut file = tempfile::Builder::new()
            .suffix(".csv")
            .tempfile()
            .expect("create temp csv");
        file.write_all(contents.as_bytes()).expect("write temp csv");
        file.flush().expect("flush temp csv");
        file
    }

    fn ppl(csv_path: &str, dst: Dbms, body: &str) -> String {
        format!(
            "connection \"src\" {{ driver = \"csv\" url = \"{csv_path}\" pk_column = \"id\" }}\n\
             {}\n{body}\n",
            dest_connection("dst", dst),
        )
    }

    const LOAD_PEOPLE: &str = r#"
        pipeline "load_people" {
            from { connection = connection.src table = "people" }
            to   { connection = connection.dst table = "people" }
            settings { create_missing_tables = true batch_size = 100 }
        }
    "#;

    /// Full load: every row lands, and inferred types preserve the values
    /// (fractional score, boolean flag).
    async fn csv_full_load_case(dst: Dbms) {
        let csv = temp_csv(PEOPLE_CSV);
        let path = csv.path().to_str().unwrap();

        let sqml = ppl(path, dst, LOAD_PEOPLE);
        run_ppl(&sqml, false).await.expect("csv migration failed");

        let url = dst.dest_url();
        assert_eq!(
            dst.count(url, "people").await,
            3,
            "[csv -> {dst}] all rows should load"
        );

        // Fractional value survives (no rounding to an integer type). Checked with
        // a range count so it is agnostic to the destination's numeric type.
        let preserved = dst
            .scalar_i64(
                url,
                &format!(
                    "SELECT COUNT(*) FROM {} WHERE id = 1 AND score > 95.4 AND score < 95.6",
                    dst.quote("people")
                ),
            )
            .await;
        assert_eq!(
            preserved, 1,
            "[csv -> {dst}] fractional score 95.5 should be preserved"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn csv_full_load_to_postgres() {
        reset_postgres_schema().await;
        csv_full_load_case(Dbms::Postgres).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn csv_full_load_to_mysql() {
        reset_mysql_dest().await;
        csv_full_load_case(Dbms::MySql).await;
    }

    /// A `where` filter on the CSV source is evaluated per row before writing.
    #[tokio::test(flavor = "multi_thread")]
    async fn csv_where_filter_to_postgres() {
        reset_postgres_schema().await;
        let csv = temp_csv(PEOPLE_CSV);
        let path = csv.path().to_str().unwrap();

        let body = r#"
            pipeline "high_scorers" {
                from { connection = connection.src table = "people" }
                to   { connection = connection.dst table = "high_scorers" }
                where "high" { people.score > 90 }
                settings { create_missing_tables = true batch_size = 100 }
            }
        "#;
        let sqml = ppl(path, Dbms::Postgres, body);
        run_ppl(&sqml, false)
            .await
            .expect("csv filter migration failed");

        // Only scores 95.5 and 99.0 pass; 88.0 is dropped.
        let got = Dbms::Postgres
            .count(Dbms::Postgres.dest_url(), "high_scorers")
            .await;
        assert_eq!(
            got, 2,
            "csv `where` filter should keep 2 of 3 rows (>90), got {got}"
        );
    }
}
