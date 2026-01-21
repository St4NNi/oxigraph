use crate::model::{GraphNameRef, NamedOrBlankNodeRef, QuadRef};
pub use crate::storage::error::{CorruptionError, LoaderError, SerializerError, StorageError};
#[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
use crate::storage::libsql::{
    LibSqlChainedDecodingQuadIterator, LibSqlDecodingGraphIterator, LibSqlStorage,
    LibSqlStorageBulkLoader, LibSqlStorageReadableTransaction, LibSqlStorageReader,
    LibSqlStorageTransaction,
};
use crate::storage::memory::{
    MemoryDecodingGraphIterator, MemoryStorage, MemoryStorageBulkLoader, MemoryStorageReader,
    MemoryStorageTransaction, QuadIterator,
};
use crate::storage::numeric_encoder::{EncodedQuad, EncodedTerm, StrHash, StrLookup};
use oxrdf::Quad;
#[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
use std::path::Path;
#[cfg(not(target_family = "wasm"))]
use std::{io, thread};

#[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
mod binary_encoder;
mod error;
#[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
mod libsql;
mod memory;
pub mod numeric_encoder;
pub mod small_string;

pub const DEFAULT_BULK_LOAD_BATCH_SIZE: usize = 1_000_000;

/// Low level storage primitives
#[derive(Clone)]
pub struct Storage {
    kind: StorageKind,
}

#[derive(Clone)]
enum StorageKind {
    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    LibSql(LibSqlStorage),
    Memory(MemoryStorage),
}

impl Storage {
    #[expect(clippy::unnecessary_wraps)]
    pub fn new() -> Result<Self, StorageError> {
        Ok(Self {
            kind: StorageKind::Memory(MemoryStorage::new()),
        })
    }

    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        Ok(Self {
            kind: StorageKind::LibSql(LibSqlStorage::open(path)?),
        })
    }

    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    pub fn open_read_only(path: &Path) -> Result<Self, StorageError> {
        Ok(Self {
            kind: StorageKind::LibSql(LibSqlStorage::open_read_only(path)?),
        })
    }

    pub fn snapshot(&self) -> StorageReader<'static> {
        StorageReader {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
                StorageKind::LibSql(storage) => StorageReaderKind::LibSql(storage.snapshot()),
                StorageKind::Memory(storage) => StorageReaderKind::Memory(storage.snapshot()),
            },
        }
    }

    #[cfg_attr(
        not(all(not(target_family = "wasm"), feature = "libsql")),
        expect(clippy::unnecessary_wraps)
    )]
    pub fn start_transaction(&self) -> Result<StorageTransaction<'_>, StorageError> {
        Ok(StorageTransaction {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
                StorageKind::LibSql(storage) => {
                    StorageTransactionKind::LibSql(storage.start_transaction()?)
                }
                StorageKind::Memory(storage) => {
                    StorageTransactionKind::Memory(storage.start_transaction())
                }
            },
        })
    }

    #[cfg_attr(
        not(all(not(target_family = "wasm"), feature = "libsql")),
        expect(clippy::unnecessary_wraps)
    )]
    pub fn start_readable_transaction(
        &self,
    ) -> Result<StorageReadableTransaction<'_>, StorageError> {
        Ok(StorageReadableTransaction {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
                StorageKind::LibSql(storage) => {
                    StorageReadableTransactionKind::LibSql(storage.start_readable_transaction()?)
                }
                StorageKind::Memory(storage) => {
                    StorageReadableTransactionKind::Memory(storage.start_transaction())
                }
            },
        })
    }

    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    pub fn flush(&self) -> Result<(), StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageKind::LibSql(storage) => storage.flush(),
            StorageKind::Memory(_) => Ok(()),
        }
    }

    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    pub fn compact(&self) -> Result<(), StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageKind::LibSql(storage) => storage.compact(),
            StorageKind::Memory(_) => Ok(()),
        }
    }

    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    pub fn backup(&self, target_directory: &Path) -> Result<(), StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageKind::LibSql(storage) => storage.backup(target_directory),
            StorageKind::Memory(_) => Err(StorageError::Other(
                "It is not possible to backup an in-memory database".into(),
            )),
        }
    }

    pub fn bulk_loader(&self) -> StorageBulkLoader<'_> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageKind::LibSql(storage) => StorageBulkLoader {
                kind: StorageBulkLoaderKind::LibSql(storage.bulk_loader()),
            },
            StorageKind::Memory(storage) => StorageBulkLoader {
                kind: StorageBulkLoaderKind::Memory(storage.bulk_loader()),
            },
        }
    }
}

#[must_use]
pub struct StorageReader<'a> {
    kind: StorageReaderKind<'a>,
}

enum StorageReaderKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    LibSql(LibSqlStorageReader),
    Memory(MemoryStorageReader<'a>),
}

#[cfg_attr(
    not(all(not(target_family = "wasm"), feature = "libsql")),
    expect(clippy::unnecessary_wraps)
)]
impl<'a> StorageReader<'a> {
    pub fn len(&self) -> Result<usize, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReaderKind::LibSql(reader) => reader.len(),
            StorageReaderKind::Memory(reader) => Ok(reader.len()),
        }
    }

    pub fn is_empty(&self) -> Result<bool, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReaderKind::LibSql(reader) => reader.is_empty(),
            StorageReaderKind::Memory(reader) => Ok(reader.is_empty()),
        }
    }

    pub fn contains(&self, quad: &EncodedQuad) -> Result<bool, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReaderKind::LibSql(reader) => reader.contains(quad),
            StorageReaderKind::Memory(reader) => Ok(reader.contains(quad)),
        }
    }

    pub fn quads_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<&EncodedTerm>,
    ) -> DecodingQuadIterator<'a> {
        DecodingQuadIterator {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
                StorageReaderKind::LibSql(reader) => DecodingQuadIteratorKind::LibSql(
                    reader.quads_for_pattern(subject, predicate, object, graph_name),
                ),
                StorageReaderKind::Memory(reader) => DecodingQuadIteratorKind::Memory(
                    reader.quads_for_pattern(subject, predicate, object, graph_name),
                ),
            },
        }
    }

    pub fn named_graphs(&self) -> DecodingGraphIterator<'a> {
        DecodingGraphIterator {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
                StorageReaderKind::LibSql(reader) => {
                    DecodingGraphIteratorKind::LibSql(reader.named_graphs())
                }
                StorageReaderKind::Memory(reader) => {
                    DecodingGraphIteratorKind::Memory(reader.named_graphs())
                }
            },
        }
    }

    pub fn contains_named_graph(&self, graph_name: &EncodedTerm) -> Result<bool, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReaderKind::LibSql(reader) => reader.contains_named_graph(graph_name),
            StorageReaderKind::Memory(reader) => Ok(reader.contains_named_graph(graph_name)),
        }
    }

    pub fn contains_str(&self, key: &StrHash) -> Result<bool, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReaderKind::LibSql(reader) => reader.contains_str(key),
            StorageReaderKind::Memory(reader) => Ok(reader.contains_str(key)),
        }
    }

    /// Validate that all the storage invariants held in the data
    pub fn validate(&self) -> Result<(), StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReaderKind::LibSql(reader) => reader.validate(),
            StorageReaderKind::Memory(reader) => reader.validate(),
        }
    }
}

#[must_use]
pub struct DecodingQuadIterator<'a> {
    kind: DecodingQuadIteratorKind<'a>,
}

enum DecodingQuadIteratorKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    LibSql(LibSqlChainedDecodingQuadIterator),
    Memory(QuadIterator<'a>),
}

impl Iterator for DecodingQuadIterator<'_> {
    type Item = Result<EncodedQuad, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            DecodingQuadIteratorKind::LibSql(iter) => iter.next(),
            DecodingQuadIteratorKind::Memory(iter) => iter.next().map(Ok),
        }
    }
}

#[must_use]
pub struct DecodingGraphIterator<'a> {
    kind: DecodingGraphIteratorKind<'a>,
}

enum DecodingGraphIteratorKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    LibSql(LibSqlDecodingGraphIterator),
    Memory(MemoryDecodingGraphIterator<'a>),
}

impl Iterator for DecodingGraphIterator<'_> {
    type Item = Result<EncodedTerm, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            DecodingGraphIteratorKind::LibSql(iter) => iter.next(),
            DecodingGraphIteratorKind::Memory(iter) => iter.next().map(Ok),
        }
    }
}

impl StrLookup for StorageReader<'_> {
    fn get_str(&self, key: &StrHash) -> Result<Option<String>, StorageError> {
        match &self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReaderKind::LibSql(reader) => reader.get_str(key),
            StorageReaderKind::Memory(reader) => reader.get_str(key),
        }
    }
}

#[must_use]
pub struct StorageTransaction<'a> {
    kind: StorageTransactionKind<'a>,
}

enum StorageTransactionKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    LibSql(LibSqlStorageTransaction<'a>),
    Memory(MemoryStorageTransaction<'a>),
}

#[cfg_attr(
    not(all(not(target_family = "wasm"), feature = "libsql")),
    expect(clippy::unnecessary_wraps)
)]
impl StorageTransaction<'_> {
    pub fn insert(&mut self, quad: QuadRef<'_>) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageTransactionKind::LibSql(transaction) => transaction.insert(quad),
            StorageTransactionKind::Memory(transaction) => {
                transaction.insert(quad);
            }
        }
    }

    pub fn insert_named_graph(&mut self, graph_name: NamedOrBlankNodeRef<'_>) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageTransactionKind::LibSql(transaction) => {
                transaction.insert_named_graph(graph_name)
            }
            StorageTransactionKind::Memory(transaction) => {
                transaction.insert_named_graph(graph_name);
            }
        }
    }

    pub fn remove(&mut self, quad: QuadRef<'_>) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageTransactionKind::LibSql(transaction) => transaction.remove(quad),
            StorageTransactionKind::Memory(transaction) => transaction.remove(quad),
        }
    }

    pub fn clear_default_graph(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageTransactionKind::LibSql(transaction) => transaction.clear_default_graph(),
            StorageTransactionKind::Memory(transaction) => {
                transaction.clear_graph(GraphNameRef::DefaultGraph)
            }
        }
    }

    pub fn clear_all_named_graphs(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageTransactionKind::LibSql(transaction) => transaction.clear_all_named_graphs(),
            StorageTransactionKind::Memory(transaction) => transaction.clear_all_named_graphs(),
        }
    }

    pub fn clear_all_graphs(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageTransactionKind::LibSql(transaction) => transaction.clear_all_graphs(),
            StorageTransactionKind::Memory(transaction) => transaction.clear_all_graphs(),
        }
    }

    pub fn remove_all_named_graphs(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageTransactionKind::LibSql(transaction) => transaction.remove_all_named_graphs(),
            StorageTransactionKind::Memory(transaction) => transaction.remove_all_named_graphs(),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageTransactionKind::LibSql(transaction) => transaction.clear(),
            StorageTransactionKind::Memory(transaction) => transaction.clear(),
        }
    }

    pub fn commit(self) -> Result<(), StorageError> {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageTransactionKind::LibSql(transaction) => transaction.commit(),
            StorageTransactionKind::Memory(transaction) => {
                transaction.commit();
                Ok(())
            }
        }
    }
}

#[must_use]
pub struct StorageReadableTransaction<'a> {
    kind: StorageReadableTransactionKind<'a>,
}

enum StorageReadableTransactionKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    LibSql(LibSqlStorageReadableTransaction<'a>),
    Memory(MemoryStorageTransaction<'a>),
}

#[cfg_attr(
    not(all(not(target_family = "wasm"), feature = "libsql")),
    expect(clippy::unnecessary_wraps)
)]
impl StorageReadableTransaction<'_> {
    pub fn reader(&self) -> StorageReader<'_> {
        StorageReader {
            kind: match &self.kind {
                #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
                StorageReadableTransactionKind::LibSql(transaction) => {
                    StorageReaderKind::LibSql(transaction.reader())
                }
                StorageReadableTransactionKind::Memory(transaction) => {
                    StorageReaderKind::Memory(transaction.reader())
                }
            },
        }
    }

    pub fn insert(&mut self, quad: QuadRef<'_>) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => transaction.insert(quad),
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.insert(quad);
            }
        }
    }

    pub fn insert_named_graph(&mut self, graph_name: NamedOrBlankNodeRef<'_>) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => {
                transaction.insert_named_graph(graph_name)
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.insert_named_graph(graph_name);
            }
        }
    }

    pub fn remove(&mut self, quad: QuadRef<'_>) {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => transaction.remove(quad),
            StorageReadableTransactionKind::Memory(transaction) => transaction.remove(quad),
        }
    }

    pub fn clear_graph(&mut self, graph_name: GraphNameRef<'_>) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => {
                transaction.clear_graph(graph_name)
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.clear_graph(graph_name);
                Ok(())
            }
        }
    }

    pub fn clear_all_named_graphs(&mut self) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => {
                transaction.clear_all_named_graphs()
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.clear_all_named_graphs();
                Ok(())
            }
        }
    }

    pub fn clear_all_graphs(&mut self) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => transaction.clear_all_graphs(),
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.clear_all_graphs();
                Ok(())
            }
        }
    }

    pub fn remove_named_graph(
        &mut self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => {
                transaction.remove_named_graph(graph_name)
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.remove_named_graph(graph_name);
                Ok(())
            }
        }
    }

    pub fn remove_all_named_graphs(&mut self) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => {
                transaction.remove_all_named_graphs()
            }
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.remove_all_named_graphs();
                Ok(())
            }
        }
    }

    pub fn clear(&mut self) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => transaction.clear(),
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.clear();
                Ok(())
            }
        }
    }

    pub fn commit(self) -> Result<(), StorageError> {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageReadableTransactionKind::LibSql(transaction) => transaction.commit(),
            StorageReadableTransactionKind::Memory(transaction) => {
                transaction.commit();
                Ok(())
            }
        }
    }
}

#[must_use]
pub struct StorageBulkLoader<'a> {
    kind: StorageBulkLoaderKind<'a>,
}

enum StorageBulkLoaderKind<'a> {
    #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
    LibSql(LibSqlStorageBulkLoader<'a>),
    Memory(MemoryStorageBulkLoader<'a>),
}

impl StorageBulkLoader<'_> {
    pub fn on_progress(self, callback: impl Fn(u64) + Send + Sync + 'static) -> Self {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageBulkLoaderKind::LibSql(loader) => Self {
                kind: StorageBulkLoaderKind::LibSql(loader.on_progress(callback)),
            },
            StorageBulkLoaderKind::Memory(loader) => Self {
                kind: StorageBulkLoaderKind::Memory(loader.on_progress(callback)),
            },
        }
    }

    pub fn without_atomicity(self) -> Self {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageBulkLoaderKind::LibSql(loader) => Self {
                kind: StorageBulkLoaderKind::LibSql(loader.without_atomicity()),
            },
            StorageBulkLoaderKind::Memory(loader) => Self {
                kind: StorageBulkLoaderKind::Memory(loader),
            },
        }
    }

    #[cfg_attr(
        any(target_family = "wasm", not(feature = "libsql")),
        expect(clippy::unnecessary_wraps, unused_variables)
    )]
    pub fn load_batch(
        &mut self,
        quads: Vec<Quad>,
        max_num_threads: usize,
    ) -> Result<(), StorageError> {
        match &mut self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageBulkLoaderKind::LibSql(loader) => loader.load_batch(quads, max_num_threads),
            StorageBulkLoaderKind::Memory(loader) => {
                loader.load_batch(quads);
                Ok(())
            }
        }
    }

    #[cfg_attr(
        any(target_family = "wasm", not(feature = "libsql")),
        expect(clippy::unnecessary_wraps)
    )]
    pub fn commit(self) -> Result<(), StorageError> {
        match self.kind {
            #[cfg(all(not(target_family = "wasm"), feature = "libsql"))]
            StorageBulkLoaderKind::LibSql(loader) => loader.commit(),
            StorageBulkLoaderKind::Memory(loader) => {
                loader.commit();
                Ok(())
            }
        }
    }
}

#[cfg(not(target_family = "wasm"))]
pub fn map_thread_result<R>(result: thread::Result<R>) -> io::Result<R> {
    result.map_err(|e| {
        io::Error::other(if let Ok(e) = e.downcast::<&dyn std::fmt::Display>() {
            format!("A loader processed crashed with {e}")
        } else {
            "A loader processed crashed with and unknown error".into()
        })
    })
}
