use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{Seek, SeekFrom, Write},
    marker::PhantomData,
    path::Path,
};

use crate::{
    cdb::{Cdb, TableEntry, HEADER_SIZE},
    hash::CdbHash,
    util::write_tuple,
    Error,
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
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
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
            entries_by_table: std::array::from_fn(|_| Vec::new()),
            is_finalized: false,
            current_data_offset: HEADER_SIZE,
            _hasher: PhantomData,
        })
    }

    /// Creates a new CdbWriter instance with a custom hasher.
    pub fn new_with_custom_hasher(mut writer: W, _hasher_instance: H) -> Result<Self, Error> {
        writer.seek(SeekFrom::Start(0))?;
        let header_placeholder = vec![0u8; HEADER_SIZE as usize];
        writer.write_all(&header_placeholder)?;

        Ok(CdbWriter {
            writer,
            entries_by_table: std::array::from_fn(|_| Vec::new()),
            is_finalized: false,
            current_data_offset: HEADER_SIZE,
            _hasher: PhantomData::<H>,
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if self.is_finalized {
            return Err(Error::WriterFinalized);
        }

        self.writer
            .seek(SeekFrom::Start(self.current_data_offset))?;
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

        self.current_data_offset += 16 + key.len() as u64 + value.len() as u64;
        Ok(())
    }

    fn write_footer_and_header_internal(&mut self) -> Result<(), Error> {
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
                length: num_slots as u64,
            };

            for entry in entries_in_this_table {
                let mut slot_idx = (entry.hash_val >> 8) % (num_slots as u64);
                loop {
                    if slots_data[slot_idx as usize].1 == 0 {
                        slots_data[slot_idx as usize] = (entry.hash_val, entry.offset);
                        break;
                    }
                    slot_idx = (slot_idx + 1) % (num_slots as u64);
                }
            }

            self.writer
                .seek(SeekFrom::Start(current_pos_for_hash_tables))?;
            for (hash_val, data_offset) in slots_data {
                write_tuple(&mut self.writer, hash_val, data_offset)?;
            }
            current_pos_for_hash_tables += (num_slots * 16) as u64;
        }

        self.writer.seek(SeekFrom::Start(0))?;
        for table_entry in final_header_entries.iter() {
            write_tuple(&mut self.writer, table_entry.offset, table_entry.length)?;
        }

        self.is_finalized = true;
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<(), Error> {
        self.write_footer_and_header_internal()?;
        self.writer.flush()?;
        Ok(())
    }

    /// Consumes the CdbWriter and returns the underlying writer.
    /// This is useful for retrieving the written data, for example, when using `Cursor<Vec<u8>>`.
    /// The writer is flushed before being returned. Finalization must have occurred.
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
    // Freeze consumes the writer, finalizes it, and reopens it as a Cdb reader.
    pub fn freeze(mut self, path_to_reopen: &Path) -> Result<Cdb<File, H>, Error> {
        self.write_footer_and_header_internal()?;
        self.writer.flush()?;

        Cdb::<File, H>::open(path_to_reopen).map_err(Error::Io)
    }
}
