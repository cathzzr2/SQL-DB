use planner::Planner;

use crate::error::Result;

use super::{
    engine::Transaction,
    executor::{Executor, ResultSet},
    parser::ast::{self, Expression},
    schema::Table,
};

mod planner;

// executable node
#[derive(Debug, PartialEq)]
pub enum Node {
    // create table
    CreateTable {
        schema: Table,
    },

    // insert data
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },

    // scan node
    Scan {
        table_name: String,
    },
}

#[derive(Debug, PartialEq)]
// define plan: with diff types of executable nodes
pub struct Plan(pub Node);

impl Plan {
    pub fn build(stmt: ast::Statement) -> Self {
        Planner::new().build(stmt)
    }

    pub fn execute<T: Transaction>(self, txn: &mut T) -> Result<ResultSet> {
        <dyn Executor<T>>::build(self.0).execute(txn)
    }
}
