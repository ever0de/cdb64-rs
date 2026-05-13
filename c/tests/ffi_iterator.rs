use std::{ffi::CString, io::Cursor, slice};

use cdb64::{Cdb, CdbHash, CdbWriter};
use tempfile::NamedTempFile;

// Reuse the C FFI functions from this crate
// import symbols from the tested crate
use cdb64_c::{
    CDB_ITERATOR_FINISHED, CDB_ITERATOR_HAS_NEXT, CDB_SUCCESS, CdbData, CdbKeyValue, cdb_close,
    cdb_free_data, cdb_get, cdb_iterator_free, cdb_iterator_new, cdb_iterator_next, cdb_open,
    cdb_writer_create, cdb_writer_finalize, cdb_writer_free, cdb_writer_put,
};

#[test]
fn ffi_iterator_integration_roundtrip() {
    // Create temp file path
    let tmp = NamedTempFile::new().expect("create temp file");
    let path = tmp.path().to_owned();
    let cpath = CString::new(path.to_string_lossy().as_bytes()).unwrap();

    unsafe {
        // create writer
        let writer = cdb_writer_create(cpath.as_ptr());
        assert!(!writer.is_null());

        // put two entries
        let k1 = b"key1";
        let v1 = b"value1";
        assert_eq!(
            cdb_writer_put(writer, k1.as_ptr(), k1.len(), v1.as_ptr(), v1.len()),
            CDB_SUCCESS
        );

        let k2 = b"key2";
        let v2 = b"value2";
        assert_eq!(
            cdb_writer_put(writer, k2.as_ptr(), k2.len(), v2.as_ptr(), v2.len()),
            CDB_SUCCESS
        );

        // finalize and free writer
        assert_eq!(cdb_writer_finalize(writer), CDB_SUCCESS);
        cdb_writer_free(writer);

        // open reader
        let reader = cdb_open(cpath.as_ptr());
        assert!(!reader.is_null());

        // create iterator — reader remains valid (Arc-shared, not consumed)
        let iter = cdb_iterator_new(reader);
        assert!(!iter.is_null());

        // iterate
        let mut found = Vec::new();
        loop {
            let mut kv = cdb64_c::test_accessors::new_empty_kv();
            let rc = cdb_iterator_next(iter, &mut kv as *mut CdbKeyValue);
            if rc == CDB_ITERATOR_HAS_NEXT {
                // read key via test accessors
                let key_ptr = cdb64_c::test_accessors::get_key_ptr(&kv);
                let key_len = cdb64_c::test_accessors::get_key_len(&kv);
                let val_ptr = cdb64_c::test_accessors::get_val_ptr(&kv);
                let val_len = cdb64_c::test_accessors::get_val_len(&kv);

                let key_slice = if !key_ptr.is_null() && key_len > 0 {
                    slice::from_raw_parts(key_ptr, key_len)
                } else {
                    &[]
                };
                let val_slice = if !val_ptr.is_null() && val_len > 0 {
                    slice::from_raw_parts(val_ptr, val_len)
                } else {
                    &[]
                };
                found.push((key_slice.to_vec(), val_slice.to_vec()));

                // take ownership and free allocated data
                let kdata = cdb64_c::test_accessors::take_key(&mut kv);
                let vdata = cdb64_c::test_accessors::take_value(&mut kv);
                cdb_free_data(kdata);
                cdb_free_data(vdata);
            } else if rc == CDB_ITERATOR_FINISHED {
                break;
            } else {
                panic!("iterator error: {}", rc);
            }
        }

        // free iterator
        cdb_iterator_free(iter);

        // The reader (CdbFile) remains valid — cdb_iterator_new now clones the Arc
        // instead of consuming the pointer, so we can still use it after the iterator is freed.
        cdb_close(reader);

        // verify
        assert!(found.contains(&(b"key1".to_vec(), b"value1".to_vec())));
        assert!(found.contains(&(b"key2".to_vec(), b"value2".to_vec())));
    }
}

/// Verify that the reader and iterator can be used simultaneously: `cdb_iterator_new`
/// must not consume / invalidate the originating `CdbFile` pointer.
#[test]
fn ffi_reader_and_iterator_coexist() {
    let tmp = NamedTempFile::new().expect("create temp file");
    let path = tmp.path().to_owned();
    let cpath = CString::new(path.to_string_lossy().as_bytes()).unwrap();

    unsafe {
        // Write
        let writer = cdb_writer_create(cpath.as_ptr());
        assert!(!writer.is_null());
        let k = b"hello";
        let v = b"world";
        assert_eq!(
            cdb_writer_put(writer, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
            CDB_SUCCESS
        );
        assert_eq!(cdb_writer_finalize(writer), CDB_SUCCESS);
        cdb_writer_free(writer);

        // Open reader
        let reader = cdb_open(cpath.as_ptr());
        assert!(!reader.is_null());

        // Create iterator — reader must still be valid afterwards
        let iter = cdb_iterator_new(reader);
        assert!(!iter.is_null());

        // Use cdb_get on the reader while the iterator is alive
        let mut value_out = cdb64_c::test_accessors::new_empty_data();
        let rc = cdb_get(reader, k.as_ptr(), k.len(), &mut value_out as *mut CdbData);
        assert_eq!(rc, CDB_SUCCESS);
        assert!(!cdb64_c::test_accessors::get_data_ptr(&value_out).is_null());
        let got = slice::from_raw_parts(
            cdb64_c::test_accessors::get_data_ptr(&value_out),
            cdb64_c::test_accessors::get_data_len(&value_out),
        );
        assert_eq!(got, b"world");
        cdb_free_data(value_out);

        // Close the reader first — the iterator still holds an Arc ref so the Cdb stays alive
        cdb_close(reader);

        // The iterator continues to work even after the CdbFile wrapper is closed
        let mut kv = cdb64_c::test_accessors::new_empty_kv();
        let rc = cdb_iterator_next(iter, &mut kv as *mut CdbKeyValue);
        assert_eq!(rc, CDB_ITERATOR_HAS_NEXT);
        let key_d = cdb64_c::test_accessors::take_key(&mut kv);
        let val_d = cdb64_c::test_accessors::take_value(&mut kv);
        cdb_free_data(key_d);
        cdb_free_data(val_d);

        cdb_iterator_free(iter);
    }
}

// ---------------------------------------------------------------------------
// Cross-binding format verification tests
// ---------------------------------------------------------------------------

/// Rust CdbWriter → C FFI reader.
///
/// The Rust library writes a CDB file; the C FFI functions must be able to
/// open it and retrieve all key-value pairs — both via `cdb_get` and via the
/// iterator — with byte-for-byte identical values.
#[test]
fn rust_write_c_read() {
    // ── write with Rust ──────────────────────────────────────────────────────
    let tmp = NamedTempFile::new().expect("create temp file");
    let path = tmp.path().to_owned();

    let entries: &[(&[u8], &[u8])] = &[
        (b"alpha", b"value_alpha"),
        (b"beta", b"value_beta"),
        // binary data with embedded null bytes
        (b"bin\x00key", b"\x00\x01\x02\x03"),
        // empty value
        (b"empty_val", b""),
        // empty key
        (b"", b"empty_key_value"),
    ];

    {
        let mut writer = CdbWriter::<_, CdbHash>::create(&path).expect("create writer");
        for (k, v) in entries {
            writer.put(k, v).expect("put");
        }
        writer.finalize().expect("finalize");
    }

    // ── read with C FFI ───────────────────────────────────────────────────────
    let cpath = CString::new(path.to_string_lossy().as_bytes()).unwrap();
    unsafe {
        let reader = cdb_open(cpath.as_ptr());
        assert!(!reader.is_null(), "cdb_open failed");

        // Point-lookup for every entry
        for (k, expected_v) in entries {
            let mut out = cdb64_c::test_accessors::new_empty_data();
            let rc = cdb_get(reader, k.as_ptr(), k.len(), &mut out as *mut CdbData);
            assert_eq!(rc, CDB_SUCCESS, "cdb_get failed for key {:?}", k);
            assert!(
                !cdb64_c::test_accessors::get_data_ptr(&out).is_null(),
                "key {:?} not found via cdb_get",
                k
            );
            let got = slice::from_raw_parts(
                cdb64_c::test_accessors::get_data_ptr(&out),
                cdb64_c::test_accessors::get_data_len(&out),
            );
            assert_eq!(got, *expected_v, "value mismatch for key {:?}", k);
            cdb_free_data(out);
        }

        // Iterator must enumerate exactly the same set of pairs
        let iter = cdb_iterator_new(reader);
        assert!(!iter.is_null());
        let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        loop {
            let mut kv = cdb64_c::test_accessors::new_empty_kv();
            let rc = cdb_iterator_next(iter, &mut kv as *mut CdbKeyValue);
            match rc {
                r if r == CDB_ITERATOR_HAS_NEXT => {
                    let k_slice = slice::from_raw_parts(
                        cdb64_c::test_accessors::get_key_ptr(&kv),
                        cdb64_c::test_accessors::get_key_len(&kv),
                    )
                    .to_vec();
                    let v_slice = slice::from_raw_parts(
                        cdb64_c::test_accessors::get_val_ptr(&kv),
                        cdb64_c::test_accessors::get_val_len(&kv),
                    )
                    .to_vec();
                    collected.push((k_slice, v_slice));
                    cdb_free_data(cdb64_c::test_accessors::take_key(&mut kv));
                    cdb_free_data(cdb64_c::test_accessors::take_value(&mut kv));
                }
                r if r == CDB_ITERATOR_FINISHED => break,
                rc => panic!("iterator error: {}", rc),
            }
        }
        cdb_iterator_free(iter);
        cdb_close(reader);

        assert_eq!(
            collected.len(),
            entries.len(),
            "iterator entry count mismatch"
        );
        for (k, v) in entries {
            assert!(
                collected.contains(&(k.to_vec(), v.to_vec())),
                "missing entry key={:?} val={:?} in iterator output",
                k,
                v
            );
        }
    }
}

/// C FFI writer → Rust reader.
///
/// The C FFI functions write a CDB file; the Rust library must open it and
/// retrieve all entries correctly — both via `Cdb::get` and the Rust iterator.
#[test]
fn c_write_rust_read() {
    // ── write with C FFI ──────────────────────────────────────────────────────
    let tmp = NamedTempFile::new().expect("create temp file");
    let path = tmp.path().to_owned();
    let cpath = CString::new(path.to_string_lossy().as_bytes()).unwrap();

    let entries: &[(&[u8], &[u8])] = &[
        (b"foo", b"bar"),
        (b"hello", b"world"),
        // binary data
        (b"\x01\x02\x03", b"\xAA\xBB\xCC\xDD"),
        // empty value
        (b"empty_val", b""),
        // empty key
        (b"", b"empty_key_value"),
    ];

    unsafe {
        let writer = cdb_writer_create(cpath.as_ptr());
        assert!(!writer.is_null(), "cdb_writer_create failed");
        for (k, v) in entries {
            let rc = cdb_writer_put(writer, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            assert_eq!(rc, CDB_SUCCESS, "cdb_writer_put failed for key {:?}", k);
        }
        assert_eq!(cdb_writer_finalize(writer), CDB_SUCCESS);
        cdb_writer_free(writer);
    }

    // ── read with Rust ────────────────────────────────────────────────────────
    let cdb = Cdb::<_, CdbHash>::open(&path).expect("Cdb::open failed");

    // Point-lookup
    for (k, expected_v) in entries {
        let got = cdb.get(k).expect("Cdb::get failed");
        assert_eq!(
            got.as_deref(),
            Some(*expected_v),
            "value mismatch for key {:?}",
            k
        );
    }

    // Iterator
    let collected: Vec<(Vec<u8>, Vec<u8>)> =
        cdb.iter().map(|r| r.expect("iterator error")).collect();
    assert_eq!(
        collected.len(),
        entries.len(),
        "iterator entry count mismatch"
    );
    for (k, v) in entries {
        assert!(
            collected.contains(&(k.to_vec(), v.to_vec())),
            "missing entry key={:?} val={:?} in Rust iterator",
            k,
            v
        );
    }
    // stabilise borrow
    drop(collected);
}

/// Duplicate keys: `cdb_get` returns the *first* inserted value; the iterator
/// returns *all* occurrences in insertion order.
#[test]
fn duplicate_keys_ffi_vs_rust() {
    // ── write ─────────────────────────────────────────────────────────────────
    let tmp = NamedTempFile::new().expect("create temp file");
    let path = tmp.path().to_owned();
    let cpath = CString::new(path.to_string_lossy().as_bytes()).unwrap();

    unsafe {
        let writer = cdb_writer_create(cpath.as_ptr());
        assert!(!writer.is_null());
        // insert "dup" key twice
        let k = b"dup".as_ref();
        let v1 = b"first".as_ref();
        let v2 = b"second".as_ref();
        assert_eq!(
            cdb_writer_put(writer, k.as_ptr(), k.len(), v1.as_ptr(), v1.len()),
            CDB_SUCCESS
        );
        assert_eq!(
            cdb_writer_put(writer, k.as_ptr(), k.len(), v2.as_ptr(), v2.len()),
            CDB_SUCCESS
        );
        assert_eq!(cdb_writer_finalize(writer), CDB_SUCCESS);
        cdb_writer_free(writer);
    }

    // ── C FFI: cdb_get returns first ──────────────────────────────────────────
    unsafe {
        let reader = cdb_open(cpath.as_ptr());
        assert!(!reader.is_null());
        let k = b"dup".as_ref();
        let mut out = cdb64_c::test_accessors::new_empty_data();
        let rc = cdb_get(reader, k.as_ptr(), k.len(), &mut out as *mut CdbData);
        assert_eq!(rc, CDB_SUCCESS);
        let got = slice::from_raw_parts(
            cdb64_c::test_accessors::get_data_ptr(&out),
            cdb64_c::test_accessors::get_data_len(&out),
        );
        assert_eq!(
            got, b"first",
            "cdb_get should return the first inserted value"
        );
        cdb_free_data(out);
        cdb_close(reader);
    }

    // ── Rust: Cdb::get returns first; iterator returns both ──────────────────
    let cdb = Cdb::<_, CdbHash>::open(&path).expect("open");
    let first = cdb.get(b"dup").expect("get").expect("key must exist");
    assert_eq!(
        first, b"first",
        "Rust Cdb::get should return the first value"
    );

    let all: Vec<_> = cdb
        .iter()
        .map(|r| r.expect("iter error"))
        .filter(|(k, _)| k == b"dup")
        .map(|(_, v)| v)
        .collect();
    assert_eq!(all.len(), 2, "iterator should yield both duplicate entries");
    assert_eq!(all[0], b"first");
    assert_eq!(all[1], b"second");
}

/// In-memory (no filesystem): write via Rust `CdbWriter<Cursor<…>>`, read via
/// Rust `Cdb::new(cursor)`, then verify through the C FFI iterator helper that
/// uses the same `ArcCdbIterator` code path.
#[test]
fn in_memory_rust_write_verify_arc_iterator() {
    use std::sync::Arc;

    let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new())).expect("new writer");
    let pairs: &[(&[u8], &[u8])] = &[(b"x", b"X"), (b"y", b"Y"), (b"bin", b"\x00\xFF\x7F")];
    for (k, v) in pairs {
        writer.put(k, v).expect("put");
    }
    writer.finalize().expect("finalize");
    let cursor = writer.into_inner().expect("into_inner");

    // Re-open the in-memory buffer as a Cdb
    let cdb = Cdb::<_, CdbHash>::new(cursor).expect("new cdb");

    // Point-lookups
    for (k, expected_v) in pairs {
        assert_eq!(
            cdb.get(k).expect("get").as_deref(),
            Some(*expected_v),
            "key {:?} mismatch",
            k
        );
    }

    // Arc-based iterator (same code path as C FFI ArcCdbIterator)
    let arc_cdb = Arc::new(cdb);
    let iter = cdb64::ArcCdbIterator::new(Arc::clone(&arc_cdb));
    let collected: Vec<_> = iter.map(|r| r.expect("iter error")).collect();

    assert_eq!(collected.len(), pairs.len());
    for (k, v) in pairs {
        assert!(
            collected.contains(&(k.to_vec(), v.to_vec())),
            "missing pair ({:?}, {:?})",
            k,
            v
        );
    }

    // Original Arc is still usable
    assert!(arc_cdb.get(b"x").unwrap().is_some());
}
