//! SQLite storage backend for Oxigraph.
//!
//! This module provides a persistent storage implementation using rusqlite (SQLite).
//! It uses a single QUAD table with multiple indexes for efficient query patterns.

// Return types match the interface in mod.rs
// Arc::clone style is a matter of preference
// Casts are safe because SQLite counts are always non-negative and within practical limits
// flush() takes &self to match the interface even though SQLite handles it automatically
#![allow(
    clippy::unnecessary_wraps,
    clippy::clone_on_ref_ptr,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::unused_self
)]

use crate::model::{GraphNameRef, NamedOrBlankNodeRef, QuadRef};
use crate::storage::binary_encoder::{decode_term, encode_term};
use crate::storage::error::StorageError;
use crate::storage::numeric_encoder::{EncodedQuad, EncodedTerm, StrHash, StrLookup, insert_term};
use oxrdf::Quad;
use rusqlite::{Connection, OptionalExtension, params};
use std::cell::RefCell;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Table names
const ID2STR_TABLE: &str = "id2str";
const GRAPHS_TABLE: &str = "graphs";
const QUADS_TABLE: &str = "quads";

/// Create all required tables for the storage schema
fn create_tables(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(&format!(
        "
        CREATE TABLE IF NOT EXISTS {ID2STR_TABLE} (hash BLOB PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS {GRAPHS_TABLE} (term BLOB PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS {QUADS_TABLE} (
            subject BLOB NOT NULL,
            predicate BLOB NOT NULL,
            object BLOB NOT NULL,
            graph_name BLOB NOT NULL,
            PRIMARY KEY (subject, predicate, object, graph_name)
        );
        CREATE INDEX IF NOT EXISTS idx_spog ON {QUADS_TABLE} (subject, predicate, object, graph_name);
        CREATE INDEX IF NOT EXISTS idx_posg ON {QUADS_TABLE} (predicate, object, subject, graph_name);
        CREATE INDEX IF NOT EXISTS idx_ospg ON {QUADS_TABLE} (object, subject, predicate, graph_name);
        CREATE INDEX IF NOT EXISTS idx_gspo ON {QUADS_TABLE} (graph_name, subject, predicate, object);
        CREATE INDEX IF NOT EXISTS idx_gpos ON {QUADS_TABLE} (graph_name, predicate, object, subject);
        CREATE INDEX IF NOT EXISTS idx_gosp ON {QUADS_TABLE} (graph_name, object, subject, predicate);
        "
    ))
    .map_err(|e| StorageError::Other(Box::new(e)))?;
    Ok(())
}

#[derive(Clone)]
pub struct LibSqlStorage {
    conn: Arc<Mutex<Connection>>,
    read_only: bool,
}

impl LibSqlStorage {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        Self::open_with_mode(path, false)
    }

    pub fn open_read_only(path: &Path) -> Result<Self, StorageError> {
        Self::open_with_mode(path, true)
    }

    fn open_with_mode(path: &Path, read_only: bool) -> Result<Self, StorageError> {
        let path_str = path.to_str().ok_or_else(|| {
            StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid path encoding",
            ))
        })?;

        let db_path = if Path::new(path_str)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("db"))
        {
            path_str.to_owned()
        } else {
            format!("{path_str}/oxigraph.db")
        };

        // Create parent directory if it doesn't exist
        if let Some(parent) = Path::new(&db_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = if read_only {
            Connection::open_with_flags(
                &db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|e| StorageError::Other(Box::new(e)))?
        } else {
            Connection::open(&db_path).map_err(|e| StorageError::Other(Box::new(e)))?
        };

        // Enable WAL mode for better concurrency
        if !read_only {
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
                .map_err(|e| StorageError::Other(Box::new(e)))?;
            create_tables(&conn)?;
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            read_only,
        })
    }

    pub fn snapshot(&self) -> LibSqlStorageReader {
        LibSqlStorageReader {
            conn: self.conn.clone(),
        }
    }

    pub fn start_transaction(&self) -> Result<LibSqlStorageTransaction<'_>, StorageError> {
        if self.read_only {
            return Err(StorageError::Other(
                "Cannot start transaction on read-only storage".into(),
            ));
        }

        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        conn.execute("BEGIN TRANSACTION", [])
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        drop(conn);

        Ok(LibSqlStorageTransaction {
            storage: self,
            operations: RefCell::new(Vec::new()),
        })
    }

    pub fn start_readable_transaction(
        &self,
    ) -> Result<LibSqlStorageReadableTransaction<'_>, StorageError> {
        if self.read_only {
            return Err(StorageError::Other(
                "Cannot start transaction on read-only storage".into(),
            ));
        }

        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        conn.execute("BEGIN TRANSACTION", [])
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        drop(conn);

        Ok(LibSqlStorageReadableTransaction {
            storage: self,
            operations: RefCell::new(Vec::new()),
        })
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        // SQLite handles flushing automatically via WAL
        Ok(())
    }

    pub fn compact(&self) -> Result<(), StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        conn.execute("VACUUM", [])
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(())
    }

    pub fn backup(&self, target_directory: &Path) -> Result<(), StorageError> {
        // Check if target directory already exists
        if target_directory.exists() {
            return Err(StorageError::Other(
                "Target backup directory already exists".into(),
            ));
        }

        // Create the target directory
        std::fs::create_dir_all(target_directory).map_err(StorageError::Io)?;

        let target_path = target_directory.join("oxigraph.db");

        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        conn.execute_batch(&format!("VACUUM INTO '{}'", target_path.display()))
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(())
    }

    pub fn bulk_loader(&self) -> LibSqlStorageBulkLoader<'_> {
        LibSqlStorageBulkLoader {
            storage: self,
            quads: Vec::new(),
            progress_callback: None,
            atomic: true,
        }
    }
}

pub struct LibSqlStorageReader {
    conn: Arc<Mutex<Connection>>,
}

impl LibSqlStorageReader {
    pub fn len(&self) -> Result<usize, StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {QUADS_TABLE}"), [], |row| {
                row.get(0)
            })
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(count as usize)
    }

    pub fn is_empty(&self) -> Result<bool, StorageError> {
        Ok(self.len()? == 0)
    }

    pub fn contains(&self, quad: &EncodedQuad) -> Result<bool, StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        let subject = encode_term(&quad.subject);
        let predicate = encode_term(&quad.predicate);
        let object = encode_term(&quad.object);
        let graph_name = encode_term(&quad.graph_name);

        let exists: Option<i64> = conn
            .query_row(
                &format!("SELECT 1 FROM {QUADS_TABLE} WHERE subject = ? AND predicate = ? AND object = ? AND graph_name = ?"),
                params![subject, predicate, object, graph_name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(exists.is_some())
    }

    pub fn quads_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<&EncodedTerm>,
    ) -> LibSqlChainedDecodingQuadIterator {
        LibSqlChainedDecodingQuadIterator::new(
            self.conn.clone(),
            subject.cloned(),
            predicate.cloned(),
            object.cloned(),
            graph_name.cloned(),
        )
    }

    pub fn named_graphs(&self) -> LibSqlDecodingGraphIterator {
        LibSqlDecodingGraphIterator::new(self.conn.clone())
    }

    pub fn contains_named_graph(&self, graph_name: &EncodedTerm) -> Result<bool, StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        let key = encode_term(graph_name);

        let exists: Option<i64> = conn
            .query_row(
                &format!("SELECT 1 FROM {GRAPHS_TABLE} WHERE term = ?"),
                [&key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(exists.is_some())
    }

    pub fn contains_str(&self, key: &StrHash) -> Result<bool, StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        let hash = key.to_be_bytes().to_vec();

        let exists: Option<i64> = conn
            .query_row(
                &format!("SELECT 1 FROM {ID2STR_TABLE} WHERE hash = ?"),
                [&hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(exists.is_some())
    }

    pub fn validate(&self) -> Result<(), StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        let result: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        if result != "ok" {
            return Err(StorageError::Other(
                format!("Database integrity check failed: {result}").into(),
            ));
        }

        Ok(())
    }
}

impl StrLookup for LibSqlStorageReader {
    fn get_str(&self, key: &StrHash) -> Result<Option<String>, StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        let hash = key.to_be_bytes().to_vec();

        let result: Option<String> = conn
            .query_row(
                &format!("SELECT value FROM {ID2STR_TABLE} WHERE hash = ?"),
                [&hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(result)
    }
}

enum TransactionOp {
    InsertQuad(EncodedQuad),
    RemoveQuad(EncodedQuad),
    InsertNamedGraph(EncodedTerm),
    RemoveNamedGraph(EncodedTerm),
    ClearGraph(EncodedTerm),
    ClearAllGraphs,
    Clear,
    InsertStr(StrHash, String),
}

pub struct LibSqlStorageTransaction<'a> {
    storage: &'a LibSqlStorage,
    operations: RefCell<Vec<TransactionOp>>,
}

impl LibSqlStorageTransaction<'_> {
    pub fn insert(&mut self, quad: QuadRef<'_>) {
        let encoded = quad.into();
        self.insert_encoded(&encoded, quad);
    }

    fn insert_encoded(&mut self, encoded: &EncodedQuad, quad: QuadRef<'_>) {
        // Store string values
        insert_term(quad.subject.into(), &encoded.subject, &mut |h, v| {
            self.operations
                .borrow_mut()
                .push(TransactionOp::InsertStr(*h, v.to_owned()));
        });
        insert_term(quad.predicate.into(), &encoded.predicate, &mut |h, v| {
            self.operations
                .borrow_mut()
                .push(TransactionOp::InsertStr(*h, v.to_owned()));
        });
        insert_term(quad.object, &encoded.object, &mut |h, v| {
            self.operations
                .borrow_mut()
                .push(TransactionOp::InsertStr(*h, v.to_owned()));
        });
        match quad.graph_name {
            GraphNameRef::NamedNode(graph_name) => {
                insert_term(graph_name.into(), &encoded.graph_name, &mut |h, v| {
                    self.operations
                        .borrow_mut()
                        .push(TransactionOp::InsertStr(*h, v.to_owned()));
                });
                // Also register the named graph
                self.operations
                    .borrow_mut()
                    .push(TransactionOp::InsertNamedGraph(encoded.graph_name.clone()));
            }
            GraphNameRef::BlankNode(graph_name) => {
                insert_term(graph_name.into(), &encoded.graph_name, &mut |h, v| {
                    self.operations
                        .borrow_mut()
                        .push(TransactionOp::InsertStr(*h, v.to_owned()));
                });
                // Also register the named graph
                self.operations
                    .borrow_mut()
                    .push(TransactionOp::InsertNamedGraph(encoded.graph_name.clone()));
            }
            GraphNameRef::DefaultGraph => (),
        }

        self.operations
            .borrow_mut()
            .push(TransactionOp::InsertQuad(encoded.clone()));
    }

    pub fn insert_named_graph(&mut self, graph_name: NamedOrBlankNodeRef<'_>) {
        let encoded: EncodedTerm = graph_name.into();
        insert_term(graph_name.into(), &encoded, &mut |h, v| {
            self.operations
                .borrow_mut()
                .push(TransactionOp::InsertStr(*h, v.to_owned()));
        });
        self.operations
            .borrow_mut()
            .push(TransactionOp::InsertNamedGraph(encoded));
    }

    pub fn remove(&mut self, quad: QuadRef<'_>) {
        let encoded: EncodedQuad = quad.into();
        self.operations
            .borrow_mut()
            .push(TransactionOp::RemoveQuad(encoded));
    }

    pub fn clear_default_graph(&mut self) {
        self.operations
            .borrow_mut()
            .push(TransactionOp::ClearGraph(EncodedTerm::DefaultGraph));
    }

    pub fn clear_all_named_graphs(&mut self) {
        self.operations
            .borrow_mut()
            .push(TransactionOp::ClearAllGraphs);
    }

    pub fn clear_all_graphs(&mut self) {
        self.clear_default_graph();
        self.clear_all_named_graphs();
    }

    pub fn remove_all_named_graphs(&mut self) {
        self.clear_all_named_graphs();
    }

    pub fn clear(&mut self) {
        self.operations.borrow_mut().push(TransactionOp::Clear);
    }

    pub fn commit(self) -> Result<(), StorageError> {
        let conn = self
            .storage
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        for op in self.operations.borrow().iter() {
            match op {
                TransactionOp::InsertQuad(quad) => {
                    insert_quad_to_db(&conn, quad)?;
                }
                TransactionOp::RemoveQuad(quad) => {
                    remove_quad_from_db(&conn, quad)?;
                }
                TransactionOp::InsertNamedGraph(graph) => {
                    insert_named_graph_to_db(&conn, graph)?;
                }
                TransactionOp::RemoveNamedGraph(graph) => {
                    remove_named_graph_from_db(&conn, graph)?;
                }
                TransactionOp::ClearGraph(graph) => {
                    clear_graph_in_db(&conn, graph)?;
                }
                TransactionOp::ClearAllGraphs => {
                    clear_all_named_graphs_in_db(&conn)?;
                }
                TransactionOp::Clear => {
                    clear_db(&conn)?;
                }
                TransactionOp::InsertStr(hash, value) => {
                    insert_str_to_db(&conn, hash, value)?;
                }
            }
        }

        conn.execute("COMMIT", [])
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(())
    }
}

pub struct LibSqlStorageReadableTransaction<'a> {
    storage: &'a LibSqlStorage,
    operations: RefCell<Vec<TransactionOp>>,
}

impl LibSqlStorageReadableTransaction<'_> {
    pub fn reader(&self) -> LibSqlStorageReader {
        LibSqlStorageReader {
            conn: self.storage.conn.clone(),
        }
    }

    pub fn insert(&mut self, quad: QuadRef<'_>) {
        let encoded = quad.into();
        self.insert_encoded(&encoded, quad);
    }

    fn insert_encoded(&mut self, encoded: &EncodedQuad, quad: QuadRef<'_>) {
        insert_term(quad.subject.into(), &encoded.subject, &mut |h, v| {
            self.operations
                .borrow_mut()
                .push(TransactionOp::InsertStr(*h, v.to_owned()));
        });
        insert_term(quad.predicate.into(), &encoded.predicate, &mut |h, v| {
            self.operations
                .borrow_mut()
                .push(TransactionOp::InsertStr(*h, v.to_owned()));
        });
        insert_term(quad.object, &encoded.object, &mut |h, v| {
            self.operations
                .borrow_mut()
                .push(TransactionOp::InsertStr(*h, v.to_owned()));
        });
        match quad.graph_name {
            GraphNameRef::NamedNode(graph_name) => {
                insert_term(graph_name.into(), &encoded.graph_name, &mut |h, v| {
                    self.operations
                        .borrow_mut()
                        .push(TransactionOp::InsertStr(*h, v.to_owned()));
                });
                // Also register the named graph
                self.operations
                    .borrow_mut()
                    .push(TransactionOp::InsertNamedGraph(encoded.graph_name.clone()));
            }
            GraphNameRef::BlankNode(graph_name) => {
                insert_term(graph_name.into(), &encoded.graph_name, &mut |h, v| {
                    self.operations
                        .borrow_mut()
                        .push(TransactionOp::InsertStr(*h, v.to_owned()));
                });
                // Also register the named graph
                self.operations
                    .borrow_mut()
                    .push(TransactionOp::InsertNamedGraph(encoded.graph_name.clone()));
            }
            GraphNameRef::DefaultGraph => (),
        }

        self.operations
            .borrow_mut()
            .push(TransactionOp::InsertQuad(encoded.clone()));
    }

    pub fn insert_named_graph(&mut self, graph_name: NamedOrBlankNodeRef<'_>) {
        let encoded: EncodedTerm = graph_name.into();
        insert_term(graph_name.into(), &encoded, &mut |h, v| {
            self.operations
                .borrow_mut()
                .push(TransactionOp::InsertStr(*h, v.to_owned()));
        });
        self.operations
            .borrow_mut()
            .push(TransactionOp::InsertNamedGraph(encoded));
    }

    pub fn remove(&mut self, quad: QuadRef<'_>) {
        let encoded: EncodedQuad = quad.into();
        self.operations
            .borrow_mut()
            .push(TransactionOp::RemoveQuad(encoded));
    }

    pub fn clear_graph(&mut self, graph_name: GraphNameRef<'_>) -> Result<(), StorageError> {
        let encoded: EncodedTerm = graph_name.into();
        self.operations
            .borrow_mut()
            .push(TransactionOp::ClearGraph(encoded));
        Ok(())
    }

    pub fn clear_all_named_graphs(&mut self) -> Result<(), StorageError> {
        self.operations
            .borrow_mut()
            .push(TransactionOp::ClearAllGraphs);
        Ok(())
    }

    pub fn clear_all_graphs(&mut self) -> Result<(), StorageError> {
        self.operations
            .borrow_mut()
            .push(TransactionOp::ClearGraph(EncodedTerm::DefaultGraph));
        self.operations
            .borrow_mut()
            .push(TransactionOp::ClearAllGraphs);
        Ok(())
    }

    pub fn remove_named_graph(
        &mut self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<(), StorageError> {
        let encoded: EncodedTerm = graph_name.into();
        self.operations
            .borrow_mut()
            .push(TransactionOp::ClearGraph(encoded.clone()));
        self.operations
            .borrow_mut()
            .push(TransactionOp::RemoveNamedGraph(encoded));
        Ok(())
    }

    pub fn remove_all_named_graphs(&mut self) -> Result<(), StorageError> {
        self.clear_all_named_graphs()
    }

    pub fn clear(&mut self) -> Result<(), StorageError> {
        self.operations.borrow_mut().push(TransactionOp::Clear);
        Ok(())
    }

    pub fn commit(self) -> Result<(), StorageError> {
        let conn = self
            .storage
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        for op in self.operations.borrow().iter() {
            match op {
                TransactionOp::InsertQuad(quad) => {
                    insert_quad_to_db(&conn, quad)?;
                }
                TransactionOp::RemoveQuad(quad) => {
                    remove_quad_from_db(&conn, quad)?;
                }
                TransactionOp::InsertNamedGraph(graph) => {
                    insert_named_graph_to_db(&conn, graph)?;
                }
                TransactionOp::RemoveNamedGraph(graph) => {
                    remove_named_graph_from_db(&conn, graph)?;
                }
                TransactionOp::ClearGraph(graph) => {
                    clear_graph_in_db(&conn, graph)?;
                }
                TransactionOp::ClearAllGraphs => {
                    clear_all_named_graphs_in_db(&conn)?;
                }
                TransactionOp::Clear => {
                    clear_db(&conn)?;
                }
                TransactionOp::InsertStr(hash, value) => {
                    insert_str_to_db(&conn, hash, value)?;
                }
            }
        }

        conn.execute("COMMIT", [])
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(())
    }
}

pub struct LibSqlStorageBulkLoader<'a> {
    storage: &'a LibSqlStorage,
    quads: Vec<Quad>,
    progress_callback: Option<Box<dyn Fn(u64) + Send + Sync>>,
    atomic: bool,
}

impl<'a> LibSqlStorageBulkLoader<'a> {
    pub fn on_progress(mut self, callback: impl Fn(u64) + Send + Sync + 'static) -> Self {
        self.progress_callback = Some(Box::new(callback));
        self
    }

    pub fn without_atomicity(mut self) -> Self {
        self.atomic = false;
        self
    }

    pub fn load_batch(
        &mut self,
        quads: Vec<Quad>,
        _max_num_threads: usize,
    ) -> Result<(), StorageError> {
        let conn = self
            .storage
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        if self.atomic && self.quads.is_empty() {
            conn.execute("BEGIN TRANSACTION", [])
                .map_err(|e| StorageError::Other(Box::new(e)))?;
        }

        for quad in &quads {
            let encoded: EncodedQuad = quad.as_ref().into();

            // Insert strings
            insert_term(
                quad.subject.as_ref().into(),
                &encoded.subject,
                &mut |h, v| {
                    drop(insert_str_to_db(&conn, h, v));
                },
            );
            insert_term(
                quad.predicate.as_ref().into(),
                &encoded.predicate,
                &mut |h, v| {
                    drop(insert_str_to_db(&conn, h, v));
                },
            );
            insert_term(quad.object.as_ref(), &encoded.object, &mut |h, v| {
                drop(insert_str_to_db(&conn, h, v));
            });
            match quad.graph_name.as_ref() {
                GraphNameRef::NamedNode(graph_name) => {
                    insert_term(graph_name.into(), &encoded.graph_name, &mut |h, v| {
                        drop(insert_str_to_db(&conn, h, v));
                    });
                }
                GraphNameRef::BlankNode(graph_name) => {
                    insert_term(graph_name.into(), &encoded.graph_name, &mut |h, v| {
                        drop(insert_str_to_db(&conn, h, v));
                    });
                }
                GraphNameRef::DefaultGraph => (),
            }

            insert_quad_to_db(&conn, &encoded)?;
        }

        self.quads.extend(quads);

        if let Some(callback) = &self.progress_callback {
            callback(self.quads.len() as u64);
        }

        Ok(())
    }

    pub fn commit(self) -> Result<(), StorageError> {
        if self.quads.is_empty() {
            return Ok(());
        }

        let conn = self
            .storage
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        if self.atomic {
            conn.execute("COMMIT", [])
                .map_err(|e| StorageError::Other(Box::new(e)))?;
        }

        Ok(())
    }
}

pub struct LibSqlChainedDecodingQuadIterator {
    conn: Arc<Mutex<Connection>>,
    subject: Option<EncodedTerm>,
    predicate: Option<EncodedTerm>,
    object: Option<EncodedTerm>,
    graph_name: Option<EncodedTerm>,
    results: Vec<EncodedQuad>,
    position: usize,
    loaded: bool,
}

impl LibSqlChainedDecodingQuadIterator {
    fn new(
        conn: Arc<Mutex<Connection>>,
        subject: Option<EncodedTerm>,
        predicate: Option<EncodedTerm>,
        object: Option<EncodedTerm>,
        graph_name: Option<EncodedTerm>,
    ) -> Self {
        Self {
            conn,
            subject,
            predicate,
            object,
            graph_name,
            results: Vec::new(),
            position: 0,
            loaded: false,
        }
    }

    fn load_results(&mut self) -> Result<(), StorageError> {
        if self.loaded {
            return Ok(());
        }
        self.loaded = true;

        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        // Build SQL query with appropriate WHERE clauses
        let mut conditions = Vec::new();
        let mut params_vec: Vec<Vec<u8>> = Vec::new();

        if let Some(s) = &self.subject {
            conditions.push("subject = ?");
            params_vec.push(encode_term(s));
        }
        if let Some(p) = &self.predicate {
            conditions.push("predicate = ?");
            params_vec.push(encode_term(p));
        }
        if let Some(o) = &self.object {
            conditions.push("object = ?");
            params_vec.push(encode_term(o));
        }
        if let Some(g) = &self.graph_name {
            conditions.push("graph_name = ?");
            params_vec.push(encode_term(g));
        }

        let query = if conditions.is_empty() {
            format!("SELECT subject, predicate, object, graph_name FROM {QUADS_TABLE}")
        } else {
            format!(
                "SELECT subject, predicate, object, graph_name FROM {QUADS_TABLE} WHERE {}",
                conditions.join(" AND ")
            )
        };

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        #[expect(trivial_casts)]
        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec
            .iter()
            .map(|p| p as &dyn rusqlite::ToSql)
            .collect();

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let subject: Vec<u8> = row.get(0)?;
                let predicate: Vec<u8> = row.get(1)?;
                let object: Vec<u8> = row.get(2)?;
                let graph_name: Vec<u8> = row.get(3)?;
                Ok((subject, predicate, object, graph_name))
            })
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        for row_result in rows {
            let (subject, predicate, object, graph_name) =
                row_result.map_err(|e| StorageError::Other(Box::new(e)))?;

            let quad = EncodedQuad {
                subject: decode_term(&subject)?,
                predicate: decode_term(&predicate)?,
                object: decode_term(&object)?,
                graph_name: if graph_name.is_empty() {
                    EncodedTerm::DefaultGraph
                } else {
                    decode_term(&graph_name)?
                },
            };
            self.results.push(quad);
        }

        Ok(())
    }
}

impl Iterator for LibSqlChainedDecodingQuadIterator {
    type Item = Result<EncodedQuad, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(e) = self.load_results() {
            return Some(Err(e));
        }

        (self.position < self.results.len()).then(|| {
            let result = self.results[self.position].clone();
            self.position += 1;
            Ok(result)
        })
    }
}

pub struct LibSqlDecodingGraphIterator {
    conn: Arc<Mutex<Connection>>,
    results: Vec<EncodedTerm>,
    position: usize,
    loaded: bool,
}

impl LibSqlDecodingGraphIterator {
    fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            results: Vec::new(),
            position: 0,
            loaded: false,
        }
    }

    fn load_results(&mut self) -> Result<(), StorageError> {
        if self.loaded {
            return Ok(());
        }
        self.loaded = true;

        let conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::Other("Failed to acquire connection lock".into()))?;

        let mut stmt = conn
            .prepare(&format!("SELECT term FROM {GRAPHS_TABLE}"))
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let rows: Vec<Vec<u8>> = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| StorageError::Other(Box::new(e)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        for term_bytes in rows {
            let term = decode_term(&term_bytes)?;
            self.results.push(term);
        }

        Ok(())
    }
}

impl Iterator for LibSqlDecodingGraphIterator {
    type Item = Result<EncodedTerm, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(e) = self.load_results() {
            return Some(Err(e));
        }

        (self.position < self.results.len()).then(|| {
            let result = self.results[self.position].clone();
            self.position += 1;
            Ok(result)
        })
    }
}

// Helper functions for database operations

fn insert_str_to_db(conn: &Connection, hash: &StrHash, value: &str) -> Result<(), StorageError> {
    let hash_bytes = hash.to_be_bytes().to_vec();
    conn.execute(
        &format!("INSERT OR IGNORE INTO {ID2STR_TABLE} (hash, value) VALUES (?, ?)"),
        params![hash_bytes, value],
    )
    .map_err(|e| StorageError::Other(Box::new(e)))?;
    Ok(())
}

fn insert_quad_to_db(conn: &Connection, quad: &EncodedQuad) -> Result<(), StorageError> {
    let subject = encode_term(&quad.subject);
    let predicate = encode_term(&quad.predicate);
    let object = encode_term(&quad.object);
    let graph_name = encode_term(&quad.graph_name);

    conn.execute(
        &format!("INSERT OR IGNORE INTO {QUADS_TABLE} (subject, predicate, object, graph_name) VALUES (?, ?, ?, ?)"),
        params![subject, predicate, object, graph_name],
    )
    .map_err(|e| StorageError::Other(Box::new(e)))?;

    Ok(())
}

fn remove_quad_from_db(conn: &Connection, quad: &EncodedQuad) -> Result<(), StorageError> {
    let subject = encode_term(&quad.subject);
    let predicate = encode_term(&quad.predicate);
    let object = encode_term(&quad.object);
    let graph_name = encode_term(&quad.graph_name);

    conn.execute(
        &format!("DELETE FROM {QUADS_TABLE} WHERE subject = ? AND predicate = ? AND object = ? AND graph_name = ?"),
        params![subject, predicate, object, graph_name],
    )
    .map_err(|e| StorageError::Other(Box::new(e)))?;

    Ok(())
}

fn insert_named_graph_to_db(conn: &Connection, graph: &EncodedTerm) -> Result<(), StorageError> {
    let term_bytes = encode_term(graph);
    conn.execute(
        &format!("INSERT OR IGNORE INTO {GRAPHS_TABLE} (term) VALUES (?)"),
        [&term_bytes],
    )
    .map_err(|e| StorageError::Other(Box::new(e)))?;
    Ok(())
}

fn remove_named_graph_from_db(conn: &Connection, graph: &EncodedTerm) -> Result<(), StorageError> {
    let term_bytes = encode_term(graph);
    conn.execute(
        &format!("DELETE FROM {GRAPHS_TABLE} WHERE term = ?"),
        [&term_bytes],
    )
    .map_err(|e| StorageError::Other(Box::new(e)))?;
    Ok(())
}

fn clear_graph_in_db(conn: &Connection, graph: &EncodedTerm) -> Result<(), StorageError> {
    let graph_name = encode_term(graph);
    conn.execute(
        &format!("DELETE FROM {QUADS_TABLE} WHERE graph_name = ?"),
        [&graph_name],
    )
    .map_err(|e| StorageError::Other(Box::new(e)))?;
    Ok(())
}

fn clear_all_named_graphs_in_db(conn: &Connection) -> Result<(), StorageError> {
    // Delete all non-default graph quads (default graph encodes as empty)
    let default_graph = encode_term(&EncodedTerm::DefaultGraph);
    conn.execute(
        &format!("DELETE FROM {QUADS_TABLE} WHERE graph_name != ?"),
        [&default_graph],
    )
    .map_err(|e| StorageError::Other(Box::new(e)))?;
    conn.execute(&format!("DELETE FROM {GRAPHS_TABLE}"), [])
        .map_err(|e| StorageError::Other(Box::new(e)))?;
    Ok(())
}

fn clear_db(conn: &Connection) -> Result<(), StorageError> {
    conn.execute(&format!("DELETE FROM {QUADS_TABLE}"), [])
        .map_err(|e| StorageError::Other(Box::new(e)))?;
    conn.execute(&format!("DELETE FROM {GRAPHS_TABLE}"), [])
        .map_err(|e| StorageError::Other(Box::new(e)))?;
    Ok(())
}
