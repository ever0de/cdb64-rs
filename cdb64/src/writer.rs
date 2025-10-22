use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{Seek, SeekFrom, Write},
    marker::PhantomData,
    path::Path,
};

use crate::{
    Error,
    cdb::{Cdb, HEADER_SIZE, TableEntry},
    hash::CdbHash,
    util::write_tuple,
};

#[derive(Debug)]
struct Entry {
    hash_val: u64,
    offset: u64,
}

pub struct CdbWriter<W: Write + Seek, H: Hasher + Default = CdbHash> {
    writer: W,
    entries_by_table: [Vec<Entry>; 256],
    is_finalized: bool,
    current_data_offset: u64,
    _hasher: PhantomData<H>,
}

impl<H: Hasher + Default> CdbWriter<File, H> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        Self::new(file)
    }
}

impl<W: Write + Seek, H: Hasher + Default> CdbWriter<W, H> {
    pub fn new(mut writer: W) -> Result<Self, Error> {
        writer.seek(SeekFrom::Start(0))?;
        let header_placeholder = vec![0u8; HEADER_SIZE as usize];
        writer.write_all(&header_placeholder)?;

        Ok(CdbWriter {
            writer,
            entries_by_table: [const { Vec::new() }; 256],
            is_finalized: false,
            current_data_offset: HEADER_SIZE,
            _hasher: PhantomData,
        })
    }

    /// Inserts a key-value pair into the CDB database.
    ///
    /// # Arguments
    ///
    /// * `key` - A byte slice representing the key
    /// * `value` - A byte slice representing the value
    ///
    /// # Behavior
    ///
    /// - **Duplicate Keys**: This method allows inserting duplicate keys. If the same key is inserted
    ///   multiple times, all values will be stored. However, `Cdb::get()` will only return the first
    ///   matching value it encounters during the hash table probe. Use iteration to retrieve all values
    ///   for duplicate keys.
    /// - **Empty Keys/Values**: Both empty keys (`b""`) and empty values are valid and supported.
    ///
    /// # Errors
    ///
    /// Returns `Error::WriterFinalized` if called after `finalize()`.
    /// Returns `Error::Io` if an I/O error occurs during writing.
    ///
    /// # Examples
    ///
    /// ```
    /// use cdb64::{CdbWriter, CdbHash};
    /// use std::io::Cursor;
    ///
    /// let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new())).unwrap();
    /// writer.put(b"key1", b"value1").unwrap();
    /// writer.put(b"key1", b"value2").unwrap(); // Duplicate key is allowed
    /// writer.put(b"", b"empty_key_value").unwrap(); // Empty key is allowed
    /// writer.finalize().unwrap();
    /// ```
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if self.is_finalized {
            return Err(Error::WriterFinalized);
        }

        self.writer
            .seek(SeekFrom::Start(self.current_data_offset))?;
        // Write key and value lengths as u64
        write_tuple(&mut self.writer, key.len() as u64, value.len() as u64)?;
        self.writer.write_all(key)?;
        self.writer.write_all(value)?;

        let mut hasher = H::default();
        hasher.write(key);
        let hash_val = hasher.finish();
        let table_idx = (hash_val & 0xff) as usize;

        self.entries_by_table[table_idx].push(Entry {
            hash_val,
            offset: self.current_data_offset,
        });

        // Adjust offset calculation: 16 bytes for (u64, u64) lengths
        self.current_data_offset += 16 + key.len() as u64 + value.len() as u64;
        Ok(())
    }

    fn write_footer_and_header(&mut self) -> Result<(), Error> {
        if self.is_finalized {
            return Ok(());
        }

        self.writer.flush()?;

        let mut final_header_entries = [TableEntry::default(); 256];
        let mut current_pos_for_hash_tables = self.current_data_offset;

        for (i, entries_in_this_table) in self.entries_by_table.iter().enumerate() {
            if entries_in_this_table.is_empty() {
                final_header_entries[i] = TableEntry {
                    offset: 0,
                    length: 0,
                };
                continue;
            }

            let num_slots = entries_in_this_table.len() * 2;
            let mut slots_data = vec![(0u64, 0u64); num_slots];

            final_header_entries[i] = TableEntry {
                offset: current_pos_for_hash_tables,
                length: num_slots as u64, // num_slots is the count of (u64, u64) pairs
            };

            for entry in entries_in_this_table {
                let mut slot_idx = (entry.hash_val >> 8) % (num_slots as u64);
                loop {
                    if slots_data[slot_idx as usize].1 == 0 {
                        // .1 is offset, 0 means empty slot
                        slots_data[slot_idx as usize] = (entry.hash_val, entry.offset);
                        break;
                    }
                    slot_idx = (slot_idx + 1) % (num_slots as u64);
                }
            }

            self.writer
                .seek(SeekFrom::Start(current_pos_for_hash_tables))?;
            for (hash_val, data_offset) in slots_data {
                // Write two u64 values directly
                self.writer.write_all(&hash_val.to_le_bytes())?;
                self.writer.write_all(&data_offset.to_le_bytes())?;
            }
            // Each slot is (u64, u64), so 16 bytes per slot. num_slots is the count of such slots.
            current_pos_for_hash_tables += (num_slots as u64) * 16;
        }

        self.writer.seek(SeekFrom::Start(0))?;
        for table_entry in final_header_entries.iter() {
            // Write two u64 values directly for the header
            self.writer.write_all(&table_entry.offset.to_le_bytes())?;
            self.writer.write_all(&table_entry.length.to_le_bytes())?;
        }

        self.is_finalized = true;

        Ok(())
    }

    pub fn finalize(&mut self) -> Result<(), Error> {
        self.write_footer_and_header()?;
        self.writer.flush()?;
        Ok(())
    }

    /// Consumes the CdbWriter and returns the underlying writer.
    ///
    /// This is useful for retrieving the written data, for example, when using `Cursor<Vec<u8>>`.
    /// The writer is flushed before being returned.
    ///
    /// # Errors
    ///
    /// Returns `Error::WriterNotFinalized` if `finalize()` has not been called yet.
    /// You must call `finalize()` before `into_inner()` to ensure the database is complete.
    ///
    /// # Examples
    ///
    /// ```
    /// use cdb64::{CdbWriter, CdbHash};
    /// use std::io::Cursor;
    ///
    /// let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new())).unwrap();
    /// writer.put(b"key", b"value").unwrap();
    /// writer.finalize().unwrap();
    ///
    /// let cursor = writer.into_inner().unwrap();
    /// let data = cursor.into_inner();
    /// assert!(data.len() > 0);
    /// ```
    pub fn into_inner(mut self) -> Result<W, Error> {
        if !self.is_finalized {
            // Ensure finalization before consuming.
            // Alternatively, one could call self.finalize() here, but it requires &mut self.
            // Forcing explicit finalize() call before into_inner() is cleaner.
            return Err(Error::WriterNotFinalized);
        }

        self.writer.flush()?; // Ensure all data is written

        Ok(self.writer)
    }
}

impl<H: Hasher + Default> CdbWriter<File, H> {
    /// Freezes the writer by finalizing it, flushing to disk, and reopening it as a `Cdb` reader.
    ///
    /// This method consumes the writer and provides a convenient way to transition from
    /// writing to reading without manually managing files.
    ///
    /// # Arguments
    ///
    /// * `path_to_reopen` - The path to reopen the file as a reader. This should be the same
    ///   path originally used to create the writer.
    ///
    /// # Errors
    ///
    /// Returns an error if finalization fails or if the file cannot be reopened.
    ///
    /// # Comparison with `finalize()` + `into_inner()`
    ///
    /// - Use `freeze()` when you want to immediately read from the same file after writing.
    /// - Use `finalize()` + `into_inner()` when you need access to the underlying writer
    ///   (e.g., to extract data from a `Cursor<Vec<u8>>`).
    ///
    /// # Examples
    ///
    /// ```
    /// use cdb64::{CdbWriter, CdbHash};
    /// use std::path::Path;
    ///
    /// let path = Path::new("my.cdb");
    /// let mut writer = CdbWriter::<_, CdbHash>::create(path).unwrap();
    /// writer.put(b"key", b"value").unwrap();
    ///
    /// // Freeze consumes the writer and returns a reader
    /// let cdb = writer.freeze(path).unwrap();
    /// assert_eq!(cdb.get(b"key").unwrap().unwrap(), b"value");
    /// # // cleanup: remove the temporary file created by the example
    /// # std::fs::remove_file(path).unwrap();
    /// ```
    // Freeze consumes the writer, finalizes it, and reopens it as a Cdb reader.
    pub fn freeze(mut self, path_to_reopen: &Path) -> Result<Cdb<File, H>, Error> {
        self.write_footer_and_header()?;
        self.writer.flush()?;

        Cdb::open(path_to_reopen).map_err(Error::Io)
    }
}
