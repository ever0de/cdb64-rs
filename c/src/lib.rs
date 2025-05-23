use cdb64::{Cdb, CdbHash, CdbWriter};
use libc::{c_char, c_int, c_uchar, size_t};
use std::ffi::CStr;
use std::fs::File;
use std::path::Path;
use std::ptr;
use std::slice; // For in-memory operations if needed

// --- Error Handling ---
// 0 for success, -1 for generic error, specific positive values for specific errors.
const CDB_SUCCESS: c_int = 0;
const CDB_ERROR_NULL_POINTER: c_int = -1;
const CDB_ERROR_IO: c_int = -3;
const CDB_ERROR_OPERATION_FAILED: c_int = -5; // General failure

// --- Writer Struct Wrapper ---
pub struct CdbWriterFile {
    writer: Option<CdbWriter<File, CdbHash>>,
}

/// # Safety
///
/// The `path` pointer must point to a valid null-terminated C string.
/// The memory pointed to by `path` must be valid for reads.
#[no_mangle]
pub unsafe extern "C" fn cdb_writer_create(path: *const c_char) -> *mut CdbWriterFile {
    if path.is_null() {
        return ptr::null_mut();
    }
    let c_str = CStr::from_ptr(path);
    let path_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(), // UTF-8 error
    };

    match CdbWriter::<File, CdbHash>::create(Path::new(path_str)) {
        Ok(writer) => Box::into_raw(Box::new(CdbWriterFile {
            writer: Some(writer),
        })),
        Err(_) => ptr::null_mut(),
    }
}

/// # Safety
///
/// `writer_ptr` must be a valid pointer to a `CdbWriterFile` obtained from `cdb_writer_create`.
/// `key_ptr` must point to a valid memory block of `key_len` bytes.
/// `value_ptr` must point to a valid memory block of `value_len` bytes.
/// The `CdbWriterFile` must not have been finalized yet.
#[no_mangle]
pub unsafe extern "C" fn cdb_writer_put(
    writer_ptr: *mut CdbWriterFile,
    key_ptr: *const c_uchar,
    key_len: size_t,
    value_ptr: *const c_uchar,
    value_len: size_t,
) -> c_int {
    if writer_ptr.is_null() || key_ptr.is_null() || value_ptr.is_null() {
        return CDB_ERROR_NULL_POINTER;
    }
    let writer_wrapper = &mut *writer_ptr;
    let writer = match writer_wrapper.writer.as_mut() {
        Some(w) => w,
        None => return CDB_ERROR_OPERATION_FAILED,
    };

    let key = slice::from_raw_parts(key_ptr, key_len);
    let value = slice::from_raw_parts(value_ptr, value_len);

    match writer.put(key, value) {
        Ok(_) => CDB_SUCCESS,
        Err(e) => {
            eprintln!("Error in cdb_writer_put: {}", e);
            CDB_ERROR_IO
        }
    }
}

/// # Safety
///
/// `writer_ptr` must be a valid pointer to a `CdbWriterFile` obtained from `cdb_writer_create`.
/// After this call, the writer is finalized, and `writer_ptr` should not be used for further `put` operations.
/// It should eventually be freed with `cdb_writer_free`.
#[no_mangle]
pub unsafe extern "C" fn cdb_writer_finalize(writer_ptr: *mut CdbWriterFile) -> c_int {
    if writer_ptr.is_null() {
        return CDB_ERROR_NULL_POINTER;
    }
    let writer_wrapper = &mut *writer_ptr;
    match writer_wrapper.writer.take() {
        // Use take to get ownership and leave None
        Some(mut writer) => {
            // writer is now owned
            match writer.finalize() {
                Ok(_) => CDB_SUCCESS,
                Err(e) => {
                    eprintln!("Error in cdb_writer_finalize: {}", e);
                    // Put the writer back if finalize failed, though it might be in a bad state
                    writer_wrapper.writer = Some(writer);
                    CDB_ERROR_IO
                }
            }
        }
        None => CDB_ERROR_OPERATION_FAILED, // Already finalized or not properly initialized
    }
}

/// # Safety
///
/// `writer_ptr` must be a valid pointer to a `CdbWriterFile` obtained from `cdb_writer_create`
/// or `ptr::null_mut()`. If it's a valid pointer, it must not be used after this function is called.
#[no_mangle]
pub unsafe extern "C" fn cdb_writer_free(writer_ptr: *mut CdbWriterFile) {
    if !writer_ptr.is_null() {
        drop(Box::from_raw(writer_ptr));
    }
}

// --- Reader Struct Wrapper ---
pub struct CdbFile {
    reader: Option<Cdb<File, CdbHash>>,
}

/// # Safety
///
/// The `path` pointer must point to a valid null-terminated C string.
/// The memory pointed to by `path` must be valid for reads.
/// The file specified by `path` must be a valid CDB file.
#[no_mangle]
pub unsafe extern "C" fn cdb_open(path: *const c_char) -> *mut CdbFile {
    if path.is_null() {
        return ptr::null_mut();
    }
    let c_str = CStr::from_ptr(path);
    let path_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    match Cdb::<File, CdbHash>::open(Path::new(path_str)) {
        Ok(reader) => Box::into_raw(Box::new(CdbFile {
            reader: Some(reader),
        })),
        Err(_) => ptr::null_mut(),
    }
}

// To return data, we need a way for C to manage the memory.
// Option 1: Caller provides buffer.
// Option 2: We allocate, caller must free using a provided function. (Chosen here)
#[repr(C)]
pub struct CdbData {
    ptr: *const c_uchar,
    len: size_t,
}

/// # Safety
///
/// `reader_ptr` must be a valid pointer to a `CdbFile` obtained from `cdb_open`.
/// `key_ptr` must point to a valid memory block of `key_len` bytes.
/// `value_out` must point to a valid `CdbData` struct where the result will be stored.
/// If the function returns `CDB_SUCCESS` and `(*value_out).ptr` is not null,
/// the memory pointed to by `(*value_out).ptr` must be freed by calling `cdb_free_data`.
#[no_mangle]
pub unsafe extern "C" fn cdb_get(
    reader_ptr: *mut CdbFile,
    key_ptr: *const c_uchar,
    key_len: size_t,
    value_out: *mut CdbData,
) -> c_int {
    if reader_ptr.is_null() || key_ptr.is_null() || value_out.is_null() {
        return CDB_ERROR_NULL_POINTER;
    }
    let reader_wrapper = &mut *reader_ptr;
    let reader = match reader_wrapper.reader.as_mut() {
        Some(r) => r,
        None => return CDB_ERROR_OPERATION_FAILED,
    };
    let key = slice::from_raw_parts(key_ptr, key_len);

    match reader.get(key) {
        Ok(Some(value_vec)) => {
            let len = value_vec.len();
            let boxed_slice = value_vec.into_boxed_slice();
            (*value_out).ptr = Box::into_raw(boxed_slice) as *const c_uchar;
            (*value_out).len = len;
            CDB_SUCCESS
        }
        Ok(None) => {
            (*value_out).ptr = ptr::null();
            (*value_out).len = 0;
            CDB_SUCCESS
        }
        Err(e) => {
            eprintln!("Error in cdb_get: {}", e);
            (*value_out).ptr = ptr::null();
            (*value_out).len = 0;
            CDB_ERROR_IO
        }
    }
}

/// # Safety
///
/// `data.ptr` must be a pointer previously obtained from `cdb_get` that has not yet been freed.
/// `data.len` must be the length associated with that pointer.
/// If `data.ptr` is null, this function does nothing.
#[no_mangle]
pub unsafe extern "C" fn cdb_free_data(data: CdbData) {
    if !data.ptr.is_null() {
        drop(Box::from_raw(slice::from_raw_parts_mut(
            data.ptr as *mut u8,
            data.len,
        )));
    }
}

/// # Safety
///
/// `reader_ptr` must be a valid pointer to a `CdbFile` obtained from `cdb_open`
/// or `ptr::null_mut()`. If it's a valid pointer, it must not be used after this function is called.
#[no_mangle]
pub unsafe extern "C" fn cdb_close(reader_ptr: *mut CdbFile) {
    if !reader_ptr.is_null() {
        drop(Box::from_raw(reader_ptr));
    }
}

// TODO: Add iterator functions if needed. This would be more complex due to lifetime management.
// For now, focusing on basic get/put.
