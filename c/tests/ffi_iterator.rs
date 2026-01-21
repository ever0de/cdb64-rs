use std::{ffi::CString, ptr, slice};

use tempfile::NamedTempFile;

// Reuse the C FFI functions from this crate
use cdb64_c::{
    cdb_writer_create, cdb_writer_put, cdb_writer_finalize, cdb_writer_free, cdb_open,
    cdb_iterator_new, cdb_iterator_next, cdb_iterator_free, cdb_free_data,
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

        // create iterator (takes ownership of reader)
        let iter = cdb_iterator_new(reader);
        assert!(!iter.is_null());

        // iterate
        let mut found = Vec::new();
        loop {
            let mut kv = CdbKeyValue { key: CdbData { ptr: ptr::null(), len: 0 }, value: CdbData { ptr: ptr::null(), len: 0 } };
            let rc = cdb_iterator_next(iter, &mut kv as *mut CdbKeyValue);
            if rc == CDB_ITERATOR_HAS_NEXT {
                // read key
                let key_slice = if !kv.key.ptr.is_null() && kv.key.len > 0 {
                    slice::from_raw_parts(kv.key.ptr, kv.key.len)
                } else {
                    &[]
                };
                let val_slice = if !kv.value.ptr.is_null() && kv.value.len > 0 {
                    slice::from_raw_parts(kv.value.ptr, kv.value.len)
                } else {
                    &[]
                };
                found.push((key_slice.to_vec(), val_slice.to_vec()));

                // free allocated data
                cdb_free_data(kv.key);
                cdb_free_data(kv.value);
            } else if rc == CDB_ITERATOR_FINISHED {
                break;
            } else {
                panic!("iterator error: {}", rc);
            }
        }

        // free iterator
        cdb_iterator_free(iter);

        // verify
        assert!(found.contains(&(b"key1".to_vec(), b"value1".to_vec())));
        assert!(found.contains(&(b"key2".to_vec(), b"value2".to_vec())));
    }
}
