use std::{ffi::CString, slice};

use tempfile::NamedTempFile;

// Reuse the C FFI functions from this crate
// import symbols from the tested crate
use cdb64_c::{
    cdb_writer_create, cdb_writer_put, cdb_writer_finalize, cdb_writer_free, cdb_open,
    cdb_get, cdb_close, cdb_free_data,
    cdb_iterator_new, cdb_iterator_next, cdb_iterator_free,
    CdbData, CdbKeyValue, CDB_SUCCESS, CDB_ITERATOR_HAS_NEXT, CDB_ITERATOR_FINISHED,
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
        assert_eq!(cdb_writer_put(writer, k1.as_ptr(), k1.len(), v1.as_ptr(), v1.len()), CDB_SUCCESS);

        let k2 = b"key2";
        let v2 = b"value2";
        assert_eq!(cdb_writer_put(writer, k2.as_ptr(), k2.len(), v2.as_ptr(), v2.len()), CDB_SUCCESS);

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
        assert_eq!(cdb_writer_put(writer, k.as_ptr(), k.len(), v.as_ptr(), v.len()), CDB_SUCCESS);
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
