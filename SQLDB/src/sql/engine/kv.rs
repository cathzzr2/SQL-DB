use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    sql::{
        schema::Table,
        types::{Row, Value},
    },
    storage::{self, engine::Engine as StorageEngine},
};

use super::{Engine, Transaction};

// Define KV Engine
pub struct KVEngine<E: StorageEngine> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self {
            kv: self.kv.clone(),
        }
    }
}

impl<E: StorageEngine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

// Define KV Transaction - MvccTransaction in storage engine
pub struct KVTransaction<E: StorageEngine> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // check line's validity
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {}
                None => {
                    return Err(Error::Internal(format!(
                        "column {} cannot be null",
                        col.name
                    )))
                }
                Some(dt) if dt != col.datatype => {
                    return Err(Error::Internal(format!(
                        "column {} type mismatch",
                        col.name
                    )))
                }
                _ => {}
            }
        }

        // Store data
        // First column as primary key for now
        // a row/entry's id - todo
        let id = Key::Row(table_name.clone(), row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(bincode::serialize(&id)?, value)?;

        Ok(())
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        let prefix = KeyPrefix::Row(table_name.clone());
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;

        let mut rows = Vec::new();
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            rows.push(row);
        }
        Ok(rows)
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // check if table exists
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} already exists",
                table.name
            )));
        }

        // check table's validity
        if table.columns.is_empty() {
            return Err(Error::Internal(format!(
                "table {} has no columns",
                table.name
            )));
        }

        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?;
        self.txn.set(bincode::serialize(&key)?, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        Ok(self
            .txn
            .get(bincode::serialize(&key)?)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),
    Row(String, Value),
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

