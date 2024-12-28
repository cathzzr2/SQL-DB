# Rust SQL Database
A simple SQL database implemented in Rust. This project explores core database functionality—from SQL parsing and planning to multi-version concurrency control (MVCC) for transactions and pluggable storage engines (in-memory or disk-based). It provides a minimal end-to-end workflow of query execution: from parsing the SQL statement to storing the results in a key-value engine.

## Key Features

1. SQL Parsing and Plan Building

A custom parser (Parser) and lexer (Lexer) handle basic SQL statements:
- CREATE TABLE
- INSERT
- SELECT
A planner converts the parsed AST into executable Nodes (e.g., CreateTable, Insert, Scan).

2. Transactional Key-Value Engine

The project defines a generic Engine trait, allowing for multiple backends:
- In-memory engine (MemoryEngine)
- Log-structured disk engine (DiskEngine) with optional compaction
An MVCC layer (storage::mvcc) on top of these engines ensures multi-version concurrency control, preventing dirty reads and write conflicts.

3. SQL Engine Abstraction

An Engine trait in sql::engine provides a high-level interface for starting transactions.
KVEngine in sql::engine::kv demonstrates how to integrate the lower-level MVCC storage with SQL operations like CREATE TABLE, INSERT, and scans.

4. Transactions and MVCC

Each transaction gets a unique version number.
The MVCC system:
- Maintains active transaction sets.
- Tracks write sets so it can roll back uncommitted data.
- Checks for conflicts (write-write conflicts) when multiple transactions modify the same key.

5. Schema and Row Handling

Table definitions (column name, type, default values).
Rows stored and fetched via the MVCC engine—translating to/from internal key formats (MvccKey).

6. Custom Serialization

Custom (de)serialization logic for keys (keycode.rs) to handle nuances like 0 bytes.
Serde-based approach for encoding data in both in-memory and disk engines.


