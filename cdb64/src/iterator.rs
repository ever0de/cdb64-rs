use std::{
    io::{self, ErrorKind},
    sync::Arc,
};

use crate::{
    cdb::{Cdb, HEADER_SIZE, TableEntry},
    util::{ReaderAt, read_tuple},
};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Compute the `(start, end)` byte positions of the data section in `cdb`.
pub(crate) fn compute_data_range<R: ReaderAt, H: std::hash::Hasher + Default>(
    cdb: &Cdb<R, H>,
) -> (u64, u64) {
    let mut end = u64::MAX;
    let mut found = false;
    for i in 0..256 {
        let entry: &TableEntry = &cdb.header[i];
        if entry.length > 0 && entry.offset >= HEADER_SIZE {
            end = end.min(entry.offset);
            found = true;
        }
    }
    (HEADER_SIZE, if found { end } else { HEADER_SIZE })
}

/// Core iteration step shared by both iterator types.
///
/// Advances `current_pos` by one record and returns the key-value pair, or
/// `None` if `current_pos >= end_pos`.  All I/O is done through the `cdb`
/// reader, so neither call-site needs to duplicate this logic.
fn advance<R: ReaderAt, H: std::hash::Hasher + Default>(
    cdb: &Cdb<R, H>,
    current_pos: &mut u64,
    end_pos: u64,
) -> Option<io::Result<(Vec<u8>, Vec<u8>)>> {
    if *current_pos >= end_pos {
        return None;
    }

    match read_tuple(&cdb.reader, *current_pos) {
        Ok((key_len, val_len)) => {
            let data_offset = match current_pos.checked_add(16) {
                Some(v) => v,
                None => {
                    return Some(Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "data offset overflow",
                    )));
                }
            };
            let record_len = match 16u64
                .checked_add(key_len)
                .and_then(|n| n.checked_add(val_len))
            {
                Some(v) => v,
                None => {
                    return Some(Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "record length overflow",
                    )));
                }
            };

            if current_pos
                .checked_add(record_len)
                .is_none_or(|end| end > end_pos)
            {
                return Some(Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "Record extends beyond expected data end",
                )));
            }

            let key_len_usize = match usize::try_from(key_len) {
                Ok(v) => v,
                Err(_) => {
                    return Some(Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "key length too large for this platform",
                    )));
                }
            };
            let val_len_usize = match usize::try_from(val_len) {
                Ok(v) => v,
                Err(_) => {
                    return Some(Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "value length too large for this platform",
                    )));
                }
            };

            let mut key_buf = vec![0u8; key_len_usize];
            if key_len > 0
                && let Err(e) = cdb.reader.read_exact_at(&mut key_buf, data_offset)
            {
                return Some(Err(e));
            }

            let val_offset = match data_offset.checked_add(key_len) {
                Some(v) => v,
                None => {
                    return Some(Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "value offset overflow",
                    )));
                }
            };
            let mut val_buf = vec![0u8; val_len_usize];
            if val_len > 0
                && let Err(e) = cdb.reader.read_exact_at(&mut val_buf, val_offset)
            {
                return Some(Err(e));
            }

            // Safety: checked_add(record_len) <= end_pos was verified above.
            *current_pos += record_len;
            Some(Ok((key_buf, val_buf)))
        }
        Err(e) => Some(Err(e)),
    }
}

// ---------------------------------------------------------------------------
// Borrow-based iterator (normal Rust API)
// ---------------------------------------------------------------------------

/// Represents a sequential iterator over a CDB database.
///
/// This iterator borrows the Cdb instance immutably for its lifetime.
///
/// # Ordering
///
/// The iterator reads records sequentially from the data section of the CDB file,
/// starting from the beginning of the data section to the end. The order depends on
/// how the CDB file was created and is **not** sorted by key or hash value.
///
/// # Duplicate Keys
///
/// If the database contains duplicate keys, all entries will be returned by the iterator.
/// Unlike `Cdb::get()`, which only returns the first match, the iterator provides access
/// to all key-value pairs including duplicates.
///
/// # Example
///
/// ```rust
/// use cdb64::{Cdb, CdbWriter, CdbHash};
/// use std::io::Cursor;
///
/// let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new())).unwrap();
/// writer.put(b"key1", b"value1").unwrap();
/// writer.put(b"key2", b"value2").unwrap();
/// writer.finalize().unwrap();
///
/// let cursor = writer.into_inner().unwrap();
/// let cdb = Cdb::<_, CdbHash>::new(cursor).unwrap();
///
/// for result in cdb.iter() {
///     let (key, value) = result.unwrap();
///     println!("Key: {:?}, Value: {:?}", key, value);
/// }
/// ```
pub struct CdbIterator<'cdb, R: ReaderAt, H: std::hash::Hasher + Default = crate::hash::CdbHash> {
    cdb: &'cdb Cdb<R, H>,
    current_pos: u64,
    end_pos: u64,
}

impl<'cdb, R: ReaderAt, H: std::hash::Hasher + Default> CdbIterator<'cdb, R, H> {
    /// Creates an iterator that borrows the Cdb immutably for its lifetime.
    pub fn new(cdb: &'cdb Cdb<R, H>) -> Self {
        let (start, end) = compute_data_range(cdb);
        CdbIterator {
            cdb,
            current_pos: start,
            end_pos: end,
        }
    }
}

impl<R: ReaderAt, H: std::hash::Hasher + Default> Iterator for CdbIterator<'_, R, H> {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        advance(self.cdb, &mut self.current_pos, self.end_pos)
    }
}

// ---------------------------------------------------------------------------
// Arc-owned iterator (FFI / shared-ownership API)
// ---------------------------------------------------------------------------

/// An iterator over a CDB database that shares ownership of the database via
/// [`Arc`].
///
/// Use this when you need an iterator that is not tied to the lifetime of a
/// particular `Cdb` reference — for example in FFI code where Rust lifetimes
/// cannot cross language boundaries, or when you want to keep both a reader
/// handle and an active iterator alive at the same time.
///
/// Unlike [`CdbIterator`], `ArcCdbIterator` does **not** borrow the `Cdb`; it
/// holds an `Arc` clone, so no lifetime parameter is required.  The underlying
/// database is kept alive until the last `Arc` reference (either the iterator's
/// own clone or the original `CdbFile` wrapper's clone) is dropped.
///
/// Data is read **lazily** on each [`Iterator::next`] call — nothing is
/// pre-loaded into memory.
///
/// # Example
///
/// ```rust
/// use cdb64::{Cdb, CdbWriter, CdbHash, ArcCdbIterator};
/// use std::{io::Cursor, sync::Arc};
///
/// let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new())).unwrap();
/// writer.put(b"key1", b"value1").unwrap();
/// writer.finalize().unwrap();
///
/// let cursor = writer.into_inner().unwrap();
/// let cdb = Arc::new(Cdb::<_, CdbHash>::new(cursor).unwrap());
///
/// // Clone the Arc — both the original handle and the iterator stay alive.
/// let iter = ArcCdbIterator::new(Arc::clone(&cdb));
/// for result in iter {
///     let (key, value) = result.unwrap();
///     println!("Key: {:?}, Value: {:?}", key, value);
/// }
/// // `cdb` is still valid here.
/// assert!(cdb.get(b"key1").unwrap().is_some());
/// ```
pub struct ArcCdbIterator<R: ReaderAt, H: std::hash::Hasher + Default = crate::hash::CdbHash> {
    cdb: Arc<Cdb<R, H>>,
    current_pos: u64,
    end_pos: u64,
}

impl<R: ReaderAt, H: std::hash::Hasher + Default> ArcCdbIterator<R, H> {
    /// Creates an iterator that shares ownership of `cdb` via an `Arc` clone.
    ///
    /// This does **not** consume `cdb`; callers retain their own `Arc` reference.
    pub fn new(cdb: Arc<Cdb<R, H>>) -> Self {
        let (start, end) = compute_data_range(&cdb);
        ArcCdbIterator {
            cdb,
            current_pos: start,
            end_pos: end,
        }
    }
}

impl<R: ReaderAt, H: std::hash::Hasher + Default> Iterator for ArcCdbIterator<R, H> {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        advance(&self.cdb, &mut self.current_pos, self.end_pos)
    }
}
