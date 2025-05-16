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
//! ```rust,no_run
//! use cdb64::{CdbWriter, Cdb, Error};
//! use tempfile::NamedTempFile;
//!
//! fn main() -> Result<(), Error> {
//!     let temp_file = NamedTempFile::new().expect("Failed to create temp file");
//!     let path = temp_file.path();
//!
//!     // Create a CDB file
//!     let mut writer = CdbWriter::create(path)?;
//!     writer.put(b"hello", b"world")?;
//!     writer.put(b"rust", b"is awesome")?;
//!     writer.finalize()?; // After finalize, writer can still be used (but put is blocked by is_finalized)
//!
//!     // Open the CDB file
//!     let cdb = Cdb::open(path)?;
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
//! ```rust,no_run
//! use cdb64::{CdbWriter, Cdb, CdbIterator, Error};
//! use tempfile::NamedTempFile;
//! use std::collections::HashMap;
//!
//! fn main() -> Result<(), Error> {
//!     let temp_file = NamedTempFile::new().expect("Failed to create temp file");
//!     let path = temp_file.path();
//!
//!     let mut data = HashMap::new();
//!     data.insert(b"key1".to_vec(), b"value1".to_vec());
//!     data.insert(b"key2".to_vec(), b"value2".to_vec());
//!
//!     let mut writer = CdbWriter::create(path)?;
//!     for (k, v) in &data {
//!         writer.put(k, v)?;
//!     }
//!     writer.finalize()?;
//!
//!     let cdb = Cdb::open(path)?;
//!     let mut iter = CdbIterator::new(cdb);
//!
//!     let mut retrieved_count = 0;
//!     while let Some(_) = iter.next() {
//!         println!("Key: {}, Value: {}",
//!             String::from_utf8_lossy(iter.key()),
//!             String::from_utf8_lossy(iter.value())
//!         );
//!         assert_eq!(data.get(iter.key()).unwrap(), iter.value());
//!         retrieved_count += 1;
//!     }
//!     assert!(iter.err().is_none());
//!     assert_eq!(retrieved_count, data.len());
//!
//!     Ok(())
//! }
//! ```

pub mod cdb;
pub mod hash;
pub mod iterator;
pub mod util;
pub mod writer;

// Re-export key public types
pub use cdb::Cdb;
pub use hash::CdbHash;
pub use iterator::CdbIterator;
pub use writer::CdbWriter;
// Re-export ReaderAt if it's meant to be part of the public API
pub use util::ReaderAt;

// Define a crate-wide Error type
/// Represents all possible errors in this crate.
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
