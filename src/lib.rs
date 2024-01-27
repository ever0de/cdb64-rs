use std::io;
#[cfg(target_os = "macos")]
use std::os::unix;

pub mod hash;
pub mod writer;

#[derive(Debug)]
pub struct Header(Vec<Table>);

#[derive(Debug)]
pub struct Table {
    pub offset: u64,
    pub length: u64,
}

pub struct CDB<F> {
    pub file: F,
    pub hasher: hash::CDBHash,
    pub header: Header,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    IoError(#[from] io::Error),

    #[error("convert: {source} to {data_type}")]
    Convert {
        source: std::array::TryFromSliceError,
        data_type: &'static str,
    },

    #[error("CDB files are limited to 4GB of data")]
    DatabaseFull,
}
impl Error {
    pub fn convert<T>(source: std::array::TryFromSliceError) -> Self {
        Self::Convert {
            data_type: std::any::type_name::<T>(),
            source,
        }
    }
}

impl<F: io::Read> CDB<F> {
    pub fn new(mut file: F) -> Result<Self, Error> {
        let header = Self::read_header(&mut file)?;

        Ok(Self {
            file,
            header,
            hasher: hash::CDBHash::new(),
        })
    }
}

impl<F: io::Read> CDB<F> {
    const HEADER_SIZE: usize = 256 * 8 * 2;

    pub fn read_header(file: &mut F) -> Result<Header, Error> {
        let mut buffer = Vec::with_capacity(Self::HEADER_SIZE);
        file.read_exact(&mut buffer)?;

        let mut header = Vec::with_capacity(256);
        for header in header.iter_mut() {
            let mut table = [0u8; 16];
            file.read_exact(&mut table)?;
            let offset = u64::from_le_bytes(table[..8].try_into().map_err(Error::convert::<u64>)?);
            let length = u64::from_le_bytes(table[8..].try_into().map_err(Error::convert::<u64>)?);

            *header = Table { offset, length };
        }

        Ok(Header(header))
    }
}

impl<F: io::Write> CDB<F> {
    pub fn write_tuple(&mut self, first: u64, second: u64) -> Result<(), Error> {
        let mut tuple = [0u8; 16];
        tuple[..8].copy_from_slice(&first.to_le_bytes());
        tuple[8..].copy_from_slice(&second.to_le_bytes());
        self.file.write_all(&tuple)?;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
impl<R: unix::fs::FileExt> CDB<R> {
    pub fn read_tuple(&self, offset: u64) -> Result<(u64, u64), Error> {
        let mut tuple = [0u8; 16];
        self.file.read_exact_at(&mut tuple, offset)?;
        let first = u64::from_le_bytes(tuple[..8].try_into().map_err(Error::convert::<u64>)?);
        let second = u64::from_le_bytes(tuple[8..].try_into().map_err(Error::convert::<u64>)?);
        Ok((first, second))
    }

    pub fn get_value_at(&self, offset: u64, expected_key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let (key_length, value_length) = self.read_tuple(offset)?;
        if key_length != expected_key.len() as u64 {
            return Ok(None);
        }

        let mut buf = vec![0u8; (key_length + value_length) as usize];
        self.file.read_exact_at(&mut buf, offset + 16)?;
        if buf[..key_length as usize] != expected_key[..] {
            return Ok(None);
        }

        Ok(Some(buf[key_length as usize..].to_vec()))
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.hasher.reset();
        self.hasher.write(key);
        let hash = self.hasher.0;

        let table = &self.header.0[(hash & 0xff) as usize];
        if table.length == 0 {
            return Ok(None);
        }

        let starting_slot = (hash >> 8) % table.length;
        let mut slot = starting_slot;

        loop {
            let slot_offset = table.offset + (16 * slot);
            let (slot_hash, offset) = self.read_tuple(slot_offset)?;

            if slot_hash == 0 {
                break;
            }

            if slot_hash == hash {
                let value = self.get_value_at(offset, key)?;
                if value.is_some() {
                    return Ok(value);
                }
            }

            slot = (slot + 1) % table.length;
            if slot == starting_slot {
                break;
            }
        }

        Ok(None)
    }
}
