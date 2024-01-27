use std::{
    fs,
    io::{self, BufWriter},
    path::Path,
};

use crate::{hash::CDBHash, Error};

pub struct Writer<W: io::Write> {
    pub hasher: CDBHash,
    pub entires: Vec<Vec<Entry>>,

    pub writer: W,
    pub buffered_offset: i64,
    pub estimated_footer_size: i64,
}

#[derive(Debug, Default)]
pub struct Entry {
    pub hash: u64,
    pub offset: u64,
}

impl Writer<BufWriter<fs::File>> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = fs::File::create(path)?;

        Ok(Self {
            hasher: CDBHash::new(),
            entires: Vec::with_capacity(256),

            writer: BufWriter::new(file),
            buffered_offset: 0,
            estimated_footer_size: 0,
        })
    }
}
