use crate::error::{Error, Result};

use super::{executor::ResultSet, parser::Parser, plan::Plan, schema::Table, types::Row};

mod kv;

// Define SQL's abstract engine layer
// Currently there is only one KVEngine
pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
        })
    }
}

// Abstract transaction info, including DDL & DML 
// can be used on regular KV engine or distributed engine later 
pub trait Transaction {
    fn commit(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
    fn create_row(&mut self, table_name: String, row: Row) -> Result<()>;
    fn scan_table(&self, table_name: String) -> Result<Vec<Row>>;
    // DDL 
    fn create_table(&mut self, table: Table) -> Result<()>;
    // Fetch table
    fn get_table(&self, table_name: String) -> Result<Option<Table>>;
    // report errors if table DNE
    fn must_get_table(&self, table_name: String) -> Result<Table> {
        self.get_table(table_name.clone())?
            .ok_or(Error::Internal(format!(
                "table {} does not exist",
                table_name
            )))
    }
}

// user end: define session
pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine> Session<E> {
    // execute user end's SQL statement
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;
                // build plan，exec SQL statement
                match Plan::build(stmt).execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    }
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }
}
