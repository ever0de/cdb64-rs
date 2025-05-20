use std::fs::File;
use std::hash::Hasher;
use std::io::{self, ErrorKind};
use std::marker::PhantomData;
use std::path::Path;

#[cfg(feature = "mmap")]
use memmap2::Mmap;

use crate::util::{ReaderAt, read_tuple};

/// The size of the CDB header in bytes.
///
/// The header consists of 256 entries, each specifying the offset and length
/// of a hash table. Each of these (offset, length) pairs consists of two u64 values,
/// so the total size is 256 * 2 * 8 = 4096 bytes.
pub const HEADER_SIZE: u64 = 256 * 8 * 2; // 256 tables, each with 2 u64s (offset, length)

/// Represents a single entry in the header's hash table.
/// Each entry points to a hash table that stores key-value pair records.
#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct TableEntry {
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

/// Represents an open CDB database. It can only be used for reads.
///
/// A `Cdb` instance provides read-only access to the database. To create or modify
/// a CDB database, use the `CdbWriter`.
///
/// The `Cdb` struct is generic over `R: ReaderAt` and `H: Hasher + Default`, allowing it to work with
/// different underlying data sources (e.g., `std::fs::File` or in-memory buffers)
/// as long as they implement the `ReaderAt` trait.
///
/// # Examples
///
/// Opening a CDB file and retrieving a value:
///
/// ```
/// use cdb64::{Cdb, CdbWriter, CdbHash};
/// use std::fs::File;
/// # use std::io::Write;
///
/// fn main() -> std::io::Result<()> {
///     # let file = File::create("test.cdb")?;
///     # let mut writer = CdbWriter::<_, CdbHash>::new(file).unwrap();
///     # writer.put(b"key", b"value").unwrap();
///     # writer.finalize().unwrap();
///     let cdb = Cdb::<_, CdbHash>::open("test.cdb")?;
///     if let Some(value) = cdb.get(b"key")? {
///         println!("Value: {:?}", value);
///     } else {
///         println!("Key not found");
///     }
///     # std::fs::remove_file("test.cdb")?;
///     Ok(())
/// }
/// ```
pub struct Cdb<R, H> {
    pub(crate) reader: R,
    pub(crate) header: [TableEntry; 256],
    _hasher: PhantomData<H>,
    #[cfg(feature = "mmap")]
    mmap: Option<Mmap>,
}

impl<H: Hasher + Default> Cdb<File, H> {
    /// Opens an existing CDB database from a file at the given path.
    ///
    /// This method initializes a `Cdb` instance with a `std::fs::File` as the reader
    /// and uses the specified `Hasher` (defaults to `CdbHash`).
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        Self::new(file)
    }

    /// Opens an existing CDB database from a file at the given path using memory-mapped I/O (mmap).
    ///
    /// This method is only available when the `mmap` feature is enabled. It opens the file, creates a memory map,
    /// and reads the CDB header using the mapped memory for efficient access. The returned `Cdb` instance keeps both
    /// the file and the mmap alive for the lifetime of the object. If the header cannot be read, an error is returned.
    #[cfg(feature = "mmap")]
    pub fn open_mmap<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let mut cdb = Cdb {
            reader: file, // Keep the file for ReaderAt, though mmap will be preferred
            header: [TableEntry::default(); 256],
            _hasher: PhantomData,
            mmap: Some(mmap),
        };
        cdb.read_header_from_mmap()?; // Read header using mmap
        Ok(cdb)
    }
}

impl<R: ReaderAt, H: Hasher + Default> Cdb<R, H> {
    /// Creates a new CDB instance using the provided `ReaderAt` and a default hasher.
    ///
    /// The hasher defaults to `H::default()`.
    pub fn new(reader: R) -> io::Result<Self> {
        let mut cdb = Cdb {
            reader,
            header: [TableEntry::default(); 256],
            _hasher: PhantomData,
            #[cfg(feature = "mmap")]
            mmap: None, // mmap is not applicable for generic ReaderAt
        };
        cdb.read_header()?;
        Ok(cdb)
    }

    /// Reads the header from the CDB file into the `Cdb` struct.
    fn read_header(&mut self) -> io::Result<()> {
        #[cfg(feature = "mmap")]
        if let Some(mmap_ref) = self.mmap.as_ref() {
            self.header = Self::read_header_from_mmap_internal(mmap_ref)?;
            return Ok(());
        }
        // Fallback to reader if mmap is not enabled or not available
        let mut header_buf = [0u8; HEADER_SIZE as usize];
        self.reader.read_exact_at(&mut header_buf, 0)?;

        for i in 0..256 {
            let offset_bytes: [u8; 8] =
                header_buf[i * 16..i * 16 + 8].try_into().map_err(|_| {
                    io::Error::new(ErrorKind::InvalidData, "Failed to slice offset from header")
                })?;
            let length_bytes: [u8; 8] =
                header_buf[i * 16 + 8..i * 16 + 16]
                    .try_into()
                    .map_err(|_| {
                        io::Error::new(ErrorKind::InvalidData, "Failed to slice length from header")
                    })?;

            self.header[i] = TableEntry {
                offset: u64::from_le_bytes(offset_bytes),
                length: u64::from_le_bytes(length_bytes),
            };
        }
        Ok(())
    }

    #[cfg(feature = "mmap")]
    fn read_header_from_mmap(&mut self) -> io::Result<()> {
        if let Some(mmap_ref) = self.mmap.as_ref() {
            self.header = Self::read_header_from_mmap_internal(mmap_ref)?;
            Ok(())
        } else {
            Err(io::Error::other("Mmap not available for reading header"))
        }
    }

    #[cfg(feature = "mmap")]
    fn read_header_from_mmap_internal(mmap_ref: &Mmap) -> io::Result<[TableEntry; 256]> {
        if mmap_ref.len() < HEADER_SIZE as usize {
            return Err(io::Error::other("Mmap data is smaller than header size"));
        }
        let header_buf = &mmap_ref[0..HEADER_SIZE as usize];
        let mut header = [TableEntry::default(); 256];

        for i in 0..256 {
            let offset_bytes: [u8; 8] =
                header_buf[i * 16..i * 16 + 8].try_into().map_err(|_| {
                    io::Error::new(
                        ErrorKind::InvalidData,
                        "Failed to slice offset from mmap header",
                    )
                })?;
            let length_bytes: [u8; 8] =
                header_buf[i * 16 + 8..i * 16 + 16]
                    .try_into()
                    .map_err(|_| {
                        io::Error::new(
                            ErrorKind::InvalidData,
                            "Failed to slice length from mmap header",
                        )
                    })?;

            header[i] = TableEntry {
                offset: u64::from_le_bytes(offset_bytes),
                length: u64::from_le_bytes(length_bytes),
            };
        }
        Ok(header)
    }

    /// Returns the value for a given key, or `None` if it can't be found.
    ///
    /// # Arguments
    ///
    /// * `key`: A byte slice representing the key to look up.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(Vec<u8>))` if the key is found, containing the associated value.
    /// * `Ok(None)` if the key is not found in the database.
    /// * `Err(io::Error)` if an I/O error occurs during the lookup process.
    ///
    /// # Process
    ///
    /// 1. Hashes the provided `key` using the configured `hasher_fn`.
    /// 2. Uses the first byte of the hash to select one of the 256 header table entries.
    /// 3. If the selected table entry is empty (length is 0), the key is not found.
    /// 4. Otherwise, probes the hash table pointed to by the header entry. The starting slot
    ///    within this table is determined by `(hash_value >> 8) % table_length`.
    /// 5. Iterates through the slots in a linear probing sequence:
    ///    1. Reads the (entry_hash, data_offset) pair from the current slot.
    ///    2. If both `entry_hash` and `data_offset` are zero, it signifies an empty slot,
    ///       and the key is considered not found (as all entries in a chain must be contiguous).
    ///    3. If `entry_hash` matches the hash of the input `key`:
    ///         1. It reads the actual key-value pair from `data_offset`.
    ///         2. If the stored key matches the input `key`, the associated value is returned.
    ///         3. If the stored key does not match (hash collision), the probing continues.
    ///    4. If `entry_hash` does not match, probing continues to the next slot.
    /// 6. If the entire hash table chain is traversed without finding the key, it returns `Ok(None)`.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let mut hasher = H::default();
        hasher.write(key);
        let hash_val = hasher.finish();

        let table_idx = (hash_val & 0xff) as usize;
        let table_entry = self.header[table_idx];

        if table_entry.length == 0 {
            return Ok(None);
        }

        let starting_slot = (hash_val >> 8) % table_entry.length;

        for i in 0..table_entry.length {
            let slot_to_check = (starting_slot + i) % table_entry.length;
            let slot_offset = table_entry.offset + slot_to_check * 16;

            #[cfg(feature = "mmap")]
            let (entry_hash, data_offset) = if let Some(mmap_ref) = self.mmap.as_ref() {
                read_tuple_from_mmap(mmap_ref, slot_offset)?
            } else {
                let mut slot_buffer = [0u8; 16];
                self.reader.read_exact_at(&mut slot_buffer, slot_offset)?;
                let h = u64::from_le_bytes(slot_buffer[0..8].try_into().map_err(|_| {
                    io::Error::new(
                        ErrorKind::InvalidData,
                        "Failed to slice entry_hash from slot",
                    )
                })?);
                let d = u64::from_le_bytes(slot_buffer[8..16].try_into().map_err(|_| {
                    io::Error::new(
                        ErrorKind::InvalidData,
                        "Failed to slice data_offset from slot",
                    )
                })?);
                (h, d)
            };

            #[cfg(not(feature = "mmap"))]
            let (entry_hash, data_offset) = {
                let mut slot_buffer = [0u8; 16];
                self.reader.read_exact_at(&mut slot_buffer, slot_offset)?;
                let h = u64::from_le_bytes(slot_buffer[0..8].try_into().map_err(|_| {
                    io::Error::new(
                        ErrorKind::InvalidData,
                        "Failed to slice entry_hash from slot",
                    )
                })?);
                let d = u64::from_le_bytes(slot_buffer[8..16].try_into().map_err(|_| {
                    io::Error::new(
                        ErrorKind::InvalidData,
                        "Failed to slice data_offset from slot",
                    )
                })?);
                (h, d)
            };

            if entry_hash == 0 && data_offset == 0 {
                return Ok(None);
            }

            if entry_hash == hash_val {
                match self.get_value_at(data_offset, key)? {
                    Some(value) => return Ok(Some(value)),
                    None => continue,
                }
            }
        }
        Ok(None)
    }

    /// Reads and verifies a key, then returns its associated value.
    /// Returns `Ok(None)` if the key at `data_offset` does not match `expected_key`.
    fn get_value_at(&self, data_offset: u64, expected_key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        #[cfg(feature = "mmap")]
        if let Some(mmap_ref) = self.mmap.as_ref() {
            return self.get_value_at_mmap(mmap_ref, data_offset, expected_key);
        }

        let (key_len, val_len) = read_tuple(&self.reader, data_offset)?;

        if key_len as usize != expected_key.len() {
            return Ok(None);
        }

        if expected_key.is_empty() {
            let mut value_buf = vec![0u8; val_len as usize];
            if val_len > 0 {
                self.reader.read_exact_at(&mut value_buf, data_offset + 8)?;
            }

            return Ok(Some(value_buf));
        }

        let mut key_buf = vec![0u8; key_len as usize];
        self.reader.read_exact_at(&mut key_buf, data_offset + 8)?;

        if key_buf != expected_key {
            return Ok(None);
        }

        let mut value_buf = vec![0u8; val_len as usize];
        if val_len > 0 {
            self.reader
                .read_exact_at(&mut value_buf, data_offset + 8 + key_len as u64)?;
        }
        Ok(Some(value_buf))
    }

    #[cfg(feature = "mmap")]
    fn get_value_at_mmap(
        &self,
        mmap_ref: &Mmap,
        data_offset: u64,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let len_offset_usize = data_offset as usize;
        if len_offset_usize + 8 > mmap_ref.len() {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Mmap bounds exceeded for key/value lengths",
            ));
        }

        let key_len_bytes: [u8; 4] = mmap_ref[len_offset_usize..len_offset_usize + 4]
            .try_into()
            .map_err(|_| {
                io::Error::new(ErrorKind::InvalidData, "Failed to slice key_len from mmap")
            })?;
        let val_len_bytes: [u8; 4] = mmap_ref[len_offset_usize + 4..len_offset_usize + 8]
            .try_into()
            .map_err(|_| {
                io::Error::new(ErrorKind::InvalidData, "Failed to slice val_len from mmap")
            })?;

        let key_len = u32::from_le_bytes(key_len_bytes);
        let val_len = u32::from_le_bytes(val_len_bytes);

        if key_len as usize != expected_key.len() {
            return Ok(None);
        }

        if expected_key.is_empty() {
            let value_buf = if val_len > 0 {
                let start = (data_offset + 8) as usize;
                let end = start + val_len as usize;
                if end > mmap_ref.len() {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "Mmap bounds exceeded for value",
                    ));
                }
                mmap_ref[start..end].to_vec()
            } else {
                Vec::new()
            };
            return Ok(Some(value_buf));
        }

        let key_start = (data_offset + 8) as usize;
        let key_end = key_start + key_len as usize;

        if key_end > mmap_ref.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "Mmap bounds exceeded for key",
            ));
        }
        let key_buf_slice = &mmap_ref[key_start..key_end];

        if key_buf_slice != expected_key {
            return Ok(None);
        }

        let value_buf = if val_len > 0 {
            let val_start = key_end;
            let val_end = val_start + val_len as usize;
            if val_end > mmap_ref.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "Mmap bounds exceeded for value",
                ));
            }
            mmap_ref[val_start..val_end].to_vec()
        } else {
            Vec::new()
        };

        Ok(Some(value_buf))
    }

    /// Returns an iterator over all key-value pairs in the database.
    ///
    /// The iterator borrows the Cdb immutably for its lifetime, so you can continue to use the Cdb while iterating.
    pub fn iter(&self) -> crate::iterator::CdbIterator<'_, R, H> {
        crate::iterator::CdbIterator::new(self)
    }
}

#[cfg(feature = "mmap")]
fn read_tuple_from_mmap(mmap: &Mmap, offset: u64) -> io::Result<(u64, u64)> {
    let start = offset as usize;
    let end = start + 16;

    if end > mmap.len() {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "Attempted to read beyond mmap bounds for tuple",
        ));
    }

    let bytes = &mmap[start..end];
    let first = u64::from_le_bytes(bytes[0..8].try_into().map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Failed to slice first u64 from mmap",
        )
    })?);
    let second = u64::from_le_bytes(bytes[8..16].try_into().map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Failed to slice second u64 from mmap",
        )
    })?);

    Ok((first, second))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::CdbHash;
    use crate::writer::CdbWriter;
    use std::hash::Hasher as StdHasher;
    use std::io::Cursor;
    #[cfg(feature = "mmap")]
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_in_memory_cdb_with_hasher<H: Hasher + Default + Clone + 'static>(
        records: &[(&[u8], &[u8])],
    ) -> Cdb<Cursor<Vec<u8>>, H> {
        let mut writer = CdbWriter::<_, H>::new(Cursor::new(Vec::new())).unwrap();
        for (key, value) in records {
            writer.put(key, value).unwrap();
        }
        writer.finalize().unwrap();
        let cursor = writer.into_inner().unwrap();
        Cdb::<_, H>::new(cursor).unwrap()
    }

    fn create_in_memory_cdb(records: &[(&[u8], &[u8])]) -> Cdb<Cursor<Vec<u8>>, CdbHash> {
        create_in_memory_cdb_with_hasher::<CdbHash>(records)
    }

    #[test]
    fn test_cdb_new_and_get_simple() {
        let records = vec![
            (b"key1".as_ref(), b"value1".as_ref()),
            (b"key2".as_ref(), b"value2".as_ref()),
        ];
        let cdb = create_in_memory_cdb(&records);

        assert_eq!(cdb.get(b"key1").unwrap().unwrap(), b"value1");
        assert_eq!(cdb.get(b"key2").unwrap().unwrap(), b"value2");
        assert!(cdb.get(b"key3").unwrap().is_none());
    }

    #[test]
    fn test_cdb_get_empty_key() {
        let records = vec![(b"".as_ref(), b"empty_value".as_ref())];
        let cdb = create_in_memory_cdb(&records);
        assert_eq!(cdb.get(b"").unwrap().unwrap(), b"empty_value");
    }

    #[test]
    fn test_cdb_get_empty_value() {
        let records = vec![(b"key_empty_val".as_ref(), b"".as_ref())];
        let cdb = create_in_memory_cdb(&records);
        assert_eq!(cdb.get(b"key_empty_val").unwrap().unwrap(), b"");
    }

    #[test]
    fn test_cdb_get_empty_key_and_value() {
        let records = vec![(b"".as_ref(), b"".as_ref())];
        let cdb = create_in_memory_cdb(&records);
        assert_eq!(cdb.get(b"").unwrap().unwrap(), b"");
    }

    #[test]
    fn test_cdb_get_from_empty_db() {
        let cdb = create_in_memory_cdb(&[]);
        assert!(cdb.get(b"any_key").unwrap().is_none());
    }

    #[test]
    fn test_cdb_open_non_existent_file() {
        let result = Cdb::<File, CdbHash>::open("non_existent_file.cdb");
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_cdb_open_and_get_from_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let file = File::create(path).unwrap();
            let mut writer = CdbWriter::<_, CdbHash>::new(file).unwrap();
            writer.put(b"file_key", b"file_value").unwrap();
            writer.finalize().unwrap();
        }

        let cdb = Cdb::<File, CdbHash>::open(path).unwrap();
        assert_eq!(cdb.get(b"file_key").unwrap().unwrap(), b"file_value");
        assert!(cdb.get(b"other_key").unwrap().is_none());

        #[cfg(feature = "mmap")]
        {
            let cdb_mmap = Cdb::<File, CdbHash>::open_mmap(path).unwrap();
            assert_eq!(cdb_mmap.get(b"file_key").unwrap().unwrap(), b"file_value");
            assert!(cdb_mmap.get(b"other_key").unwrap().is_none());
        }
    }

    #[derive(Clone, Default)]
    struct CollisionHasher {
        state: u64,
    }

    impl StdHasher for CollisionHasher {
        fn finish(&self) -> u64 {
            if self.state == u64::from_le_bytes(*b"key_A   ") {
                0x0102030405060708
            } else if self.state == u64::from_le_bytes(*b"key_B   ") {
                0x1112131415161718
            } else if self.state == u64::from_le_bytes(*b"key_C   ") {
                0x0102030405060708
            } else {
                self.state
            }
        }

        fn write(&mut self, bytes: &[u8]) {
            if bytes.len() <= 8 {
                let mut arr = [0u8; 8];
                arr[..bytes.len()].copy_from_slice(bytes);
                self.state = u64::from_le_bytes(arr);
            } else {
                self.state = 0xDEADBEEFCAFEFACE;
            }
        }
    }

    #[test]
    fn test_cdb_get_with_hash_collision() {
        let records = [
            (b"key_A".as_ref(), b"value_A".as_ref()),
            (b"key_B".as_ref(), b"value_B".as_ref()),
            (b"key_C".as_ref(), b"value_C".as_ref()),
        ];
        let cdb = create_in_memory_cdb_with_hasher::<CollisionHasher>(&records);

        assert_eq!(cdb.get(b"key_A").unwrap().unwrap(), b"value_A");
        assert_eq!(cdb.get(b"key_B").unwrap().unwrap(), b"value_B");
        assert_eq!(cdb.get(b"key_C").unwrap().unwrap(), b"value_C");
        assert!(cdb.get(b"key_D").unwrap().is_none());
    }

    #[test]
    fn test_read_header_invalid_data_short() {
        let data = vec![0u8; HEADER_SIZE as usize - 10];
        let cursor = Cursor::new(data.clone());
        let result = Cdb::<_, CdbHash>::new(cursor);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), ErrorKind::UnexpectedEof);

        #[cfg(feature = "mmap")]
        {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();
            {
                let mut file = File::create(path).unwrap();
                file.write_all(&data).unwrap();
            }
            let result_mmap = Cdb::<File, CdbHash>::open_mmap(path);
            assert!(result_mmap.is_err());
            let err_kind = result_mmap.err().unwrap().kind();
            assert!(
                err_kind == ErrorKind::InvalidData || err_kind == ErrorKind::Other,
                "Unexpected error kind: {:?}",
                err_kind
            );
        }
    }

    #[test]
    fn test_header_size_value() {
        assert_eq!(HEADER_SIZE, 256 * 8 * 2);
    }
}
