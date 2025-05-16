use cdb64::{Cdb, CdbHash, CdbIterator, CdbWriter, Error};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use tempfile::NamedTempFile;

// A simple custom hasher for testing
#[derive(Clone)]
struct CustomTestHasher {
    state: u64,
}

impl Default for CustomTestHasher {
    fn default() -> Self {
        Self { state: 12345 } // Different start state from CdbHash for distinction
    }
}

impl Hasher for CustomTestHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            // A very simple hashing algorithm for testing purposes
            self.state = self
                .state
                .rotate_left(5)
                .wrapping_add(*byte as u64)
                .wrapping_add(0x67);
        }
    }
}

#[test]
fn test_read_write_simple() -> Result<(), Error> {
    let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
    let file_path = temp_file.path();

    let mut writer = CdbWriter::<_, CdbHash>::create(file_path)?;
    writer.put(b"hello", b"world")?;
    writer.put(b"rust", b"is awesome")?;
    writer.put(b"", b"empty key")?; // Test empty key
    writer.put(b"key with empty value", b"")?; // Test empty value
    writer.finalize()?;

    let cdb = Cdb::<_, CdbHash>::open(file_path)?;

    // Test get
    assert_eq!(cdb.get(b"hello")?.unwrap(), b"world");
    assert_eq!(cdb.get(b"rust")?.unwrap(), b"is awesome");
    assert_eq!(cdb.get(b"")?.unwrap(), b"empty key");
    assert_eq!(cdb.get(b"key with empty value")?.unwrap(), b"");
    assert!(cdb.get(b"nonexistent")?.is_none());

    Ok(())
}

#[test]
fn test_read_write_iterator() -> Result<(), Error> {
    let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
    let file_path = temp_file.path();

    let mut original_data = HashMap::new();
    original_data.insert(b"key1".to_vec(), b"value1".to_vec());
    original_data.insert(b"key2".to_vec(), b"value2_longervalue".to_vec());
    original_data.insert(b"key3".to_vec(), b"value3".to_vec());
    original_data.insert(b"anotherkey".to_vec(), b"anothervalue".to_vec());
    original_data.insert(b"".to_vec(), b"empty_key_value".to_vec());

    let mut writer = CdbWriter::<_, CdbHash>::create(file_path)?;
    for (k, v) in &original_data {
        writer.put(k, v)?;
    }
    writer.finalize()?;

    let cdb = Cdb::<_, CdbHash>::open(file_path)?;
    let mut iter = CdbIterator::new(cdb);

    let mut count = 0;
    while let Some(_) = iter.next() {
        let key = iter.key();
        let value = iter.value();
        assert_eq!(original_data.get(key).unwrap(), value);
        count += 1;
    }
    assert!(
        iter.err().is_none(),
        "Iterator encountered an error: {:?}",
        iter.err()
    );
    assert_eq!(count, original_data.len());

    Ok(())
}

#[test]
fn test_get_non_existent_key() -> Result<(), Error> {
    let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
    let file_path = temp_file.path();

    let mut writer = CdbWriter::<_, CdbHash>::create(file_path)?;
    writer.put(b"exists", b"yes")?;
    writer.finalize()?;

    let cdb = Cdb::<_, CdbHash>::open(file_path)?;
    assert!(cdb.get(b"does_not_exist")?.is_none());
    Ok(())
}

#[test]
fn test_empty_database() -> Result<(), Error> {
    let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
    let file_path = temp_file.path();

    let mut writer = CdbWriter::<_, CdbHash>::create(file_path)?; // Changed back to let mut writer
    writer.finalize()?; // Finalize an empty writer

    let cdb = Cdb::<_, CdbHash>::open(file_path)?;
    assert!(cdb.get(b"any_key")?.is_none());

    let mut iter = CdbIterator::new(cdb);
    assert!(iter.next().is_none());
    assert!(iter.err().is_none());

    Ok(())
}

#[test]
fn test_freeze_and_reopen() -> Result<(), Error> {
    let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
    let file_path = temp_file.path();

    let mut writer = CdbWriter::<_, CdbHash>::create(file_path)?;
    writer.put(b"freeze_key", b"freeze_value")?;

    // Freeze the writer and reopen as Cdb
    let cdb = writer.freeze(file_path)?;

    assert_eq!(cdb.get(b"freeze_key")?.unwrap(), b"freeze_value");
    assert!(cdb.get(b"nonexistent_after_freeze")?.is_none());
    Ok(())
}

#[test]
fn test_read_write_custom_hasher() -> Result<(), Error> {
    let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
    let file_path = temp_file.path();

    // Create CdbWriter with CustomTestHasher
    let file_for_writer = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_path)?;
    let mut writer = CdbWriter::<_, CustomTestHasher>::new(file_for_writer)?; // Explicitly use CustomTestHasher

    writer.put(b"custom_key1", b"custom_value1")?;
    writer.put(b"custom_key2", b"custom_value2")?;
    writer.put(b"", b"empty_custom_key")?;
    writer.finalize()?;

    // Open Cdb with CustomTestHasher
    let file_for_reader = File::open(file_path)?;
    let cdb_custom = Cdb::<_, CustomTestHasher>::new(file_for_reader)?; // Explicitly use CustomTestHasher

    assert_eq!(cdb_custom.get(b"custom_key1")?.unwrap(), b"custom_value1");
    assert_eq!(cdb_custom.get(b"custom_key2")?.unwrap(), b"custom_value2");
    assert_eq!(cdb_custom.get(b"")?.unwrap(), b"empty_custom_key");
    assert!(cdb_custom.get(b"nonexistent_custom")?.is_none());

    // Sanity check: try opening with default CdbHash, keys should not be found
    // (or if found due to collision, values would likely be wrong)
    // This requires reopening the file, as Cdb::open takes ownership of the path or file.
    let cdb_default_hasher = Cdb::<_, CdbHash>::open(file_path)?;
    assert!(
        cdb_default_hasher.get(b"custom_key1")?.is_none(),
        "Key should not be found with default hasher"
    );
    assert!(
        cdb_default_hasher.get(b"")?.is_none(),
        "Empty key should not be found with default hasher"
    );

    Ok(())
}

#[test]
fn test_freeze_custom_hasher() -> Result<(), Error> {
    let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
    let file_path = temp_file.path();

    let file_for_writer = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_path)?;
    // Use new_with_custom_hasher or new with turbofish
    let mut writer = CdbWriter::<_, CustomTestHasher>::new(file_for_writer)?;
    writer.put(b"freeze_custom", b"value_custom")?;

    // Freeze consumes writer. path_to_reopen is &Path.
    let cdb_custom = writer.freeze(file_path)?;

    assert_eq!(cdb_custom.get(b"freeze_custom")?.unwrap(), b"value_custom");
    assert!(cdb_custom.get(b"nonexistent_freeze_custom")?.is_none());
    Ok(())
}
