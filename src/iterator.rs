use std::io::{self, ErrorKind};

use crate::cdb::{Cdb, TableEntry, HEADER_SIZE}; // Now Cdb, HEADER_SIZE, and TableEntry are available
use crate::util::{read_tuple, ReaderAt};

/// Represents a sequential iterator over a CDB database.
pub struct CdbIterator<R: ReaderAt> {
    cdb: Cdb<R>, // Store a Cdb instance to access its reader and header
    current_pos: u64,
    end_pos: u64,
    current_key: Option<Vec<u8>>,
    current_value: Option<Vec<u8>>,
    error: Option<io::Error>,
}

impl<R: ReaderAt> CdbIterator<R> {
    /// Creates an `Iterator` that can be used to iterate the database.
    /// The `Cdb` instance is moved into the iterator.
    pub fn new(cdb: Cdb<R>) -> Self {
        // Determine the end position for iteration.
        // Records are stored sequentially after the main header (HEADER_SIZE)
        // and before the hash tables begin.
        // The start of the first hash table marks the end of the data records area.
        let mut calculated_end_pos = u64::MAX;
        let mut has_valid_table_offset = false;

        // Access the header from the Cdb struct directly.
        // The `header` field in `Cdb` is now `pub(crate)`.
        for i in 0..256 {
            let table_entry: &TableEntry = &cdb.header[i];
            if table_entry.length > 0 && table_entry.offset > 0 && table_entry.offset >= HEADER_SIZE
            {
                calculated_end_pos = std::cmp::min(calculated_end_pos, table_entry.offset);
                has_valid_table_offset = true;
            }
        }

        let end_pos = if has_valid_table_offset {
            calculated_end_pos
        } else {
            // If no valid table offsets, implies no data records after the header.
            HEADER_SIZE
        };

        CdbIterator {
            cdb,
            current_pos: HEADER_SIZE,
            end_pos,
            current_key: None,
            current_value: None,
            error: None,
        }
    }

    /// Returns the current key. Panics if called after `next()` returns `false` or before `next()` is called.
    pub fn key(&self) -> &[u8] {
        self.current_key
            .as_ref()
            .expect("No current key. Call next() first or check its return value.")
            .as_slice()
    }

    /// Returns the current value. Panics if called after `next()` returns `false` or before `next()` is called.
    pub fn value(&self) -> &[u8] {
        self.current_value
            .as_ref()
            .expect("No current value. Call next() first or check its return value.")
            .as_slice()
    }

    /// Returns the current error, if any occurred during iteration.
    pub fn err(&self) -> Option<&io::Error> {
        self.error.as_ref()
    }
}

impl<R: ReaderAt> Iterator for CdbIterator<R> {
    type Item = (); // We expose key/value through methods, not as iterator item itself to allow error handling.

    /// Advances the iterator and reads the next key/value pair.
    /// Returns `Some(())` if a record was successfully read. The key and value can then be accessed via `key()` and `value()`.
    /// Returns `None` if the end of the database is reached or an error occurs.
    /// After `None` is returned, `err()` should be checked for any errors.
    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() || self.current_pos >= self.end_pos {
            self.current_key = None;
            self.current_value = None;
            return None;
        }

        match read_tuple(&self.cdb.reader, self.current_pos) {
            Ok((key_len, val_len)) => {
                let record_data_offset = self.current_pos + 16; // Data starts after the 16-byte length tuple
                let total_record_len_without_header = key_len + val_len;
                let total_record_len_with_header = 16 + total_record_len_without_header;

                // Check if the full record would read past end_pos.
                if self
                    .current_pos
                    .saturating_add(total_record_len_with_header)
                    > self.end_pos
                {
                    self.error = Some(io::Error::new(
                        ErrorKind::InvalidData,
                        "Record extends beyond expected data end",
                    ));
                    self.current_key = None;
                    self.current_value = None;
                    return None;
                }

                let mut key_buf = vec![0u8; key_len as usize];
                if key_len > 0 {
                    if let Err(e) = self
                        .cdb
                        .reader
                        .read_exact_at(&mut key_buf, record_data_offset)
                    {
                        self.error = Some(e);
                        self.current_key = None;
                        self.current_value = None;
                        return None;
                    }
                }
                self.current_key = Some(key_buf);

                let mut val_buf = vec![0u8; val_len as usize];
                if val_len > 0 {
                    if let Err(e) = self
                        .cdb
                        .reader
                        .read_exact_at(&mut val_buf, record_data_offset + key_len)
                    {
                        self.error = Some(e);
                        self.current_key = None; // Clear key as well on error
                        self.current_value = None;
                        return None;
                    }
                }
                self.current_value = Some(val_buf);

                self.current_pos += total_record_len_with_header;
                Some(())
            }
            Err(e) => {
                // If read_tuple fails because it tries to read past EOF, it might be a normal end of iteration
                // if current_pos was exactly at end_pos and end_pos marked the true end of file or data.
                // However, the loop condition self.current_pos >= self.end_pos should catch this.
                // So, an error from read_tuple here is likely a genuine I/O error or malformed data.
                self.error = Some(e);
                self.current_key = None;
                self.current_value = None;
                None
            }
        }
    }
}
