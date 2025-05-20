use std::io::{self, ErrorKind};

use crate::cdb::{Cdb, HEADER_SIZE, TableEntry};
use crate::util::{ReaderAt, read_tuple};

/// Represents a sequential iterator over a CDB database.
///
/// This iterator borrows the Cdb instance immutably for its lifetime.
pub struct CdbIterator<'cdb, R: ReaderAt, H: std::hash::Hasher + Default = crate::hash::CdbHash> {
    cdb: &'cdb Cdb<R, H>,
    current_pos: u64,
    end_pos: u64,
}

impl<'cdb, R: ReaderAt, H: std::hash::Hasher + Default> CdbIterator<'cdb, R, H> {
    /// Creates an iterator that borrows the Cdb immutably for its lifetime.
    pub fn new(cdb: &'cdb Cdb<R, H>) -> Self {
        let mut calculated_end_pos = u64::MAX;
        let mut has_valid_table_offset = false;
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
            HEADER_SIZE
        };

        CdbIterator {
            cdb,
            current_pos: HEADER_SIZE,
            end_pos,
        }
    }
}

impl<'a, R: ReaderAt, H: std::hash::Hasher + Default> Iterator for CdbIterator<'a, R, H> {
    type Item = Result<(Vec<u8>, Vec<u8>), io::Error>;

    /// Advances the iterator and reads the next key/value pair.
    /// Returns `Some(Ok((key, value)))` if a record was successfully read.
    /// Returns `Some(Err(e))` if an error occurs.
    /// Returns `None` if the end of the database is reached.
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.end_pos {
            return None;
        }

        match read_tuple(&self.cdb.reader, self.current_pos) {
            Ok((key_len, val_len)) => {
                let record_data_offset = self.current_pos + 16;
                let total_record_len_with_header = 16 + key_len + val_len;

                if self
                    .current_pos
                    .saturating_add(total_record_len_with_header)
                    > self.end_pos
                {
                    return Some(Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "Record extends beyond expected data end",
                    )));
                }

                let mut key_buf = vec![0u8; key_len as usize];
                if key_len > 0 {
                    if let Err(e) = self
                        .cdb
                        .reader
                        .read_exact_at(&mut key_buf, record_data_offset)
                    {
                        return Some(Err(e));
                    }
                }

                let mut val_buf = vec![0u8; val_len as usize];
                if val_len > 0 {
                    if let Err(e) = self
                        .cdb
                        .reader
                        .read_exact_at(&mut val_buf, record_data_offset + key_len)
                    {
                        return Some(Err(e));
                    }
                }
                self.current_pos += total_record_len_with_header;

                Some(Ok((key_buf, val_buf)))
            }
            Err(e) => Some(Err(e)),
        }
    }
}
