use crate::sql::base::error::DbError;

pub enum Transaction<'a> {
    PgTransaction(tokio_postgres::Transaction<'a>),
    MySqlTransaction(mysql_async::Transaction<'a>),
}

impl<'a> Transaction<'a> {
    pub async fn commit(self) -> Result<(), DbError> {
        match self {
            Transaction::PgTransaction(tx) => {
                tx.commit().await?;
                Ok(())
            }
            Transaction::MySqlTransaction(tx) => {
                tx.commit().await?;
                Ok(())
            }
        }
    }
}
