use std::collections::HashMap;

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::Expression,
        schema::Table,
        types::{Row, Value},
    },
};

use super::{Executor, ResultSet};

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            columns,
            values,
        })
    }
}

// column aligned
// tbl:
// insert into tbl values(1, 2, 3);
// a       b       c          d
// 1       2       3       default 
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default) = &column.default {
            results.push(default.clone());
        } else {
            return Err(Error::Internal(format!(
                "No default value for column {}",
                column.name
            )));
        }
    }

    Ok(results)
}

// tbl:
// insert into tbl(d, c) values(1, 2);
//    a          b       c          d
// default   default     2          1
fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    // check if # of cols = value
    if columns.len() != values.len() {
        return Err(Error::Internal(format!("columns and values num mismatch")));
    }

    let mut inputs = HashMap::new();
    for (i, col_name) in columns.iter().enumerate() {
        inputs.insert(col_name, values[i].clone());
    }

    let mut results = Vec::new();
    for col in table.columns.iter() {
        if let Some(value) = inputs.get(&col.name) {
            results.push(value.clone());
        } else if let Some(value) = &col.default {
            results.push(value.clone());
        } else {
            return Err(Error::Internal(format!(
                "No value given for the column {}",
                col.name
            )));
        }
    }

    Ok(results)
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let mut count = 0;
        // fetch table info 
        let table = txn.must_get_table(self.table_name.clone())?;
        for exprs in self.values {
            // convert expression into value
            let row = exprs
                .into_iter()
                .map(|e| Value::from_expression(e))
                .collect::<Vec<_>>();
            // if not inserted into the specified col
            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
            } else {
                // inserted into the specified col,
                // organize value
                make_row(&table, &self.columns, &row)?
            };

            // insert data
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }

        Ok(ResultSet::Insert { count })
    }
}
