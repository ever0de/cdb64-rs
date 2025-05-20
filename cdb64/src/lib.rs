//! # cdb64
//!
//! `cdb64` is a Rust implementation of D. J. Bernstein's cdb (constant database) format,
//! specifically designed to handle large database files efficiently using 64-bit hash values and offsets.
//!
//! This library provides `CdbWriter` for creating cdb files and `Cdb` for reading existing cdb files.
//! It also includes `CdbIterator` for iterating over all key-value pairs within a database.
//!
//! ## Features
//!
//! - CDB file creation (`CdbWriter`)
//! - CDB file reading and key lookups (`Cdb`)
//! - Database iteration (`CdbIterator`)
//! - Support for custom hash functions (defaults to CDB hash)
//!
//! ## Usage Examples
//!
//! ### Creating and Reading a CDB File
//!
//! ```rust
//! use cdb64::{CdbWriter, Cdb, Error, CdbHash};
//! use tempfile::NamedTempFile;
//! use std::fs::File;
//!
//! fn main() -> Result<(), Error> {
//!     let temp_file = NamedTempFile::new().expect("Failed to create temp file");
//!     let path = temp_file.path();
//!
//!     // Create a CDB file
//!     let mut writer = CdbWriter::<File, CdbHash>::create(path)?;
//!     writer.put(b"hello", b"world")?;
//!     writer.put(b"rust", b"is awesome")?;
//!     writer.finalize()?; // After finalize, writer can still be used (but put is blocked by is_finalized)
//!
//!     // Open the CDB file
//!     let cdb = Cdb::<File, CdbHash>::open(path)?;
//!
//!     // Retrieve a value by key
//!     if let Some(value) = cdb.get(b"hello")? {
//!         println!("Found value: {}", String::from_utf8_lossy(&value));
//!     }
//!
//!     // Search for a non-existent key
//!     assert!(cdb.get(b"nonexistent")?.is_none());
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Using the Iterator
//!
//! ```rust
//! use cdb64::{CdbWriter, Cdb, CdbIterator, Error, CdbHash};
//! use tempfile::NamedTempFile;
//! use std::collections::HashMap;
//! use std::fs::File;
//!
//! fn main() -> Result<(), Error> {
//!     let temp_file = NamedTempFile::new().expect("Failed to create temp file");
//!     let path = temp_file.path();
//!
//!     let mut data = HashMap::new();
//!     data.insert(b"key1".to_vec(), b"value1".to_vec());
//!     data.insert(b"key2".to_vec(), b"value2".to_vec());
//!
//!     let mut writer = CdbWriter::<File, CdbHash>::create(path)?;
//!     for (k, v) in &data {
//!         writer.put(k, v)?;
//!     }
//!     writer.finalize()?;
//!
//!     let cdb: Cdb<_, CdbHash> = Cdb::open(path)?;
//!     let iter = cdb.iter();
//!
//!     let mut retrieved_count = 0;
//!     for result in iter {
//!         let (key, value) = result?;
//!         println!("Key: {}, Value: {}",
//!             String::from_utf8_lossy(&key),
//!             String::from_utf8_lossy(&value)
//!         );
//!         assert_eq!(data.get(&key).unwrap(), &value);
//!         retrieved_count += 1;
//!     }
//!     assert_eq!(retrieved_count, data.len());
//!
//!     Ok(())
//! }
//! ```

mod cdb;
mod hash;
mod iterator;
mod util;
mod writer;

// re-exports
pub use cdb::Cdb;
pub use hash::CdbHash;
pub use iterator::CdbIterator;
pub use util::ReaderAt;
pub use writer::CdbWriter;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An error occurred during data conversion, typically when trying to convert a slice to an array.
    #[error("Data conversion error: {source} when converting to {data_type}")]
    Convert {
        /// The underlying slice conversion error.
        source: std::array::TryFromSliceError,
        /// The name of the type that the conversion was attempting to create.
        data_type: &'static str,
    },

    /// Indicates that the database is full.
    /// CDB files have a size limit related to internal offsets or counts.
    #[error(
        "Database is full: CDB files have a size limit (related to internal offsets or counts)"
    )]
    DatabaseFull,

    /// Indicates an attempt to operate on a writer that has already been finalized.
    #[error("Attempted to operate on a finalized writer")]
    WriterFinalized,

    /// Indicates an attempt to use a writer that has not been finalized yet when finalization is required.
    #[error("Writer has not been finalized yet")]
    WriterNotFinalized,

    /// An internal error, often indicating a bug or unexpected state.
    #[error("Internal error: {0}")]
    Internal(String),
}
