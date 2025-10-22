use cdb64::{Cdb, CdbHash, CdbWriter, Error};
use std::io::Cursor;
use tempfile::NamedTempFile;

/// Test handling of duplicate keys.
/// CDB allows duplicate keys - all are stored but get() returns the first match.
#[test]
fn test_duplicate_keys() -> Result<(), Error> {
    let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
    let file_path = temp_file.path();

    let mut writer = CdbWriter::<_, CdbHash>::create(file_path)?;
    writer.put(b"duplicate", b"value1")?;
    writer.put(b"duplicate", b"value2")?;
    writer.put(b"duplicate", b"value3")?;
    writer.put(b"other", b"other_value")?;
    writer.finalize()?;

    let cdb = Cdb::<_, CdbHash>::open(file_path)?;

    // get() should return the first matching value
    let value = cdb.get(b"duplicate")?.expect("Key should exist");
    assert_eq!(value, b"value1", "get() should return first value");

    // Iterator should return all duplicate entries
    let duplicates: Vec<_> = cdb
        .iter()
        .filter_map(|r| r.ok())
        .filter(|(k, _)| k == b"duplicate")
        .collect();

    assert_eq!(duplicates.len(), 3, "All duplicates should be in iteration");
    assert_eq!(duplicates[0].1, b"value1");
    assert_eq!(duplicates[1].1, b"value2");
    assert_eq!(duplicates[2].1, b"value3");

    Ok(())
}

/// Test with very long keys and values (testing u64 length handling).
#[test]
fn test_large_key_value() -> Result<(), Error> {
    let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
    let file_path = temp_file.path();

    // Create keys and values with significant sizes
    let large_key = vec![b'k'; 10_000]; // 10KB key
    let large_value = vec![b'v'; 100_000]; // 100KB value

    let mut writer = CdbWriter::<_, CdbHash>::create(file_path)?;
    writer.put(&large_key, &large_value)?;
    writer.put(b"small", b"value")?;
    writer.finalize()?;

    let cdb = Cdb::<_, CdbHash>::open(file_path)?;

    let retrieved_value = cdb.get(&large_key)?.expect("Large key should exist");
    assert_eq!(retrieved_value.len(), large_value.len());
    assert_eq!(retrieved_value, large_value);

    let small_value = cdb.get(b"small")?.expect("Small key should exist");
    assert_eq!(small_value, b"value");

    Ok(())
}

/// Test behavior with many entries in a single hash table bucket.
#[test]
fn test_many_entries_same_table() -> Result<(), Error> {
    let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new()))?;

    // Insert 1000 entries - some will collide in the same hash table
    for i in 0..1_000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        writer.put(key.as_bytes(), value.as_bytes())?;
    }
    writer.finalize()?;

    let cursor = writer.into_inner()?;
    let cdb = Cdb::<_, CdbHash>::new(cursor)?;

    // Verify all entries can be retrieved
    for i in 0..1_000 {
        let key = format!("key_{:04}", i);
        let expected_value = format!("value_{:04}", i);
        let value = cdb
            .get(key.as_bytes())?
            .unwrap_or_else(|| panic!("Key {} should exist", key));
        assert_eq!(
            value,
            expected_value.as_bytes(),
            "Value mismatch for key {}",
            key
        );
    }

    Ok(())
}

/// Test iteration order is consistent with insertion order.
#[test]
fn test_iteration_order() -> Result<(), Error> {
    let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new()))?;

    let entries = vec![
        (b"first".to_vec(), b"1".to_vec()),
        (b"second".to_vec(), b"2".to_vec()),
        (b"third".to_vec(), b"3".to_vec()),
        (b"fourth".to_vec(), b"4".to_vec()),
    ];

    for (k, v) in &entries {
        writer.put(k, v)?;
    }
    writer.finalize()?;

    let cursor = writer.into_inner()?;
    let cdb = Cdb::<_, CdbHash>::new(cursor)?;

    let retrieved: Vec<_> = cdb.iter().collect::<Result<Vec<_>, _>>()?;

    assert_eq!(retrieved.len(), entries.len());
    for (i, (expected_k, expected_v)) in entries.iter().enumerate() {
        assert_eq!(
            &retrieved[i].0, expected_k,
            "Key order mismatch at index {}",
            i
        );
        assert_eq!(
            &retrieved[i].1, expected_v,
            "Value order mismatch at index {}",
            i
        );
    }

    Ok(())
}

/// Test empty database iteration.
#[test]
fn test_empty_database_iteration() -> Result<(), Error> {
    let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new()))?;
    writer.finalize()?;

    let cursor = writer.into_inner()?;
    let cdb = Cdb::<_, CdbHash>::new(cursor)?;

    let count = cdb.iter().count();
    assert_eq!(count, 0, "Empty database should have no entries");

    Ok(())
}

/// Test database with only empty keys and values.
#[test]
fn test_only_empty_keys_values() -> Result<(), Error> {
    let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new()))?;
    writer.put(b"", b"")?;
    writer.put(b"", b"value")?;
    writer.put(b"key", b"")?;
    writer.finalize()?;

    let cursor = writer.into_inner()?;
    let cdb = Cdb::<_, CdbHash>::new(cursor)?;

    // First empty key with empty value
    let value1 = cdb.get(b"")?.expect("Empty key should exist");
    assert_eq!(value1, b"", "First empty key should have empty value");

    // Key with empty value
    let value2 = cdb.get(b"key")?.expect("Key should exist");
    assert_eq!(value2, b"", "Key should have empty value");

    // Check iteration returns all entries
    let all: Vec<_> = cdb.iter().collect::<Result<Vec<_>, _>>()?;
    assert_eq!(all.len(), 3, "Should have all 3 entries");

    Ok(())
}

/// Test attempt to use writer after finalization.
#[test]
fn test_writer_after_finalize() -> Result<(), Error> {
    let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new()))?;
    writer.put(b"key", b"value")?;
    writer.finalize()?;

    // Attempting to put after finalize should fail
    let result = writer.put(b"another", b"value");
    assert!(result.is_err(), "Put after finalize should fail");

    match result {
        Err(Error::WriterFinalized) => {} // Expected
        _ => panic!("Expected WriterFinalized error"),
    }

    Ok(())
}

/// Test into_inner without finalize should fail.
#[test]
fn test_into_inner_without_finalize() {
    let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new())).unwrap();
    writer.put(b"key", b"value").unwrap();

    // Should fail because finalize wasn't called
    let result = writer.into_inner();
    assert!(result.is_err(), "into_inner without finalize should fail");

    match result {
        Err(Error::WriterNotFinalized) => {} // Expected
        _ => panic!("Expected WriterNotFinalized error"),
    }
}

/// Test that all 256 hash tables can be used.
#[test]
fn test_all_hash_tables_coverage() -> Result<(), Error> {
    use std::{collections::HashSet, hash::Hasher};

    let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new()))?;

    // Generate keys that will hash to different tables (low byte determines table)
    // We need to find at least one key for each of the 256 tables
    let mut tables_hit = HashSet::new();
    let mut key_num = 0u32;

    while tables_hit.len() < 256 && key_num < 100_000 {
        let key = format!("key_{}", key_num);
        let mut hasher = CdbHash::default();
        hasher.write(key.as_bytes());
        let hash = hasher.finish();
        let table_idx = (hash & 0xff) as usize;

        if !tables_hit.contains(&table_idx) {
            writer.put(key.as_bytes(), b"value")?;
            tables_hit.insert(table_idx);
        }
        key_num += 1;
    }

    assert_eq!(
        tables_hit.len(),
        256,
        "Should be able to generate keys for all 256 tables"
    );

    writer.finalize()?;
    let cursor = writer.into_inner()?;
    let cdb = Cdb::<_, CdbHash>::new(cursor)?;

    // Verify all entries are retrievable
    key_num = 0;
    let mut verified_count = 0;
    while verified_count < 256 && key_num < 100_000 {
        let key = format!("key_{}", key_num);
        if cdb.get(key.as_bytes())?.is_some() {
            verified_count += 1;
        }
        key_num += 1;
    }

    assert_eq!(verified_count, 256, "All entries should be retrievable");

    Ok(())
}
