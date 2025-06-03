use cdb64::{Cdb, CdbHash, CdbWriter};
use libc::{c_char, c_int, c_uchar, size_t};
use std::ffi::CStr;
use std::fs::File;
use std::path::Path;
use std::ptr;
use std::slice;

// --- Error Handling ---
// 0 for success, -1 for generic error, specific positive values for specific errors.
pub const CDB_SUCCESS: c_int = 0;
pub const CDB_ERROR_NULL_POINTER: c_int = -1;
pub const CDB_ERROR_IO: c_int = -3;
pub const CDB_ERROR_OPERATION_FAILED: c_int = -5; // General failure

// --- Writer Struct Wrapper ---
pub struct CdbWriterFile {
    writer: Option<CdbWriter<File, CdbHash>>,
}

/// # Safety
///
/// The `path` pointer must point to a valid null-terminated C string.
/// The memory pointed to by `path` must be valid for reads.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_writer_create(path: *const c_char) -> *mut CdbWriterFile {
    if path.is_null() {
        return ptr::null_mut();
    }
    let c_str = unsafe { CStr::from_ptr(path) };
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
#[unsafe(no_mangle)]
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
    let writer_wrapper = unsafe { &mut *writer_ptr };
    let writer = match writer_wrapper.writer.as_mut() {
        Some(w) => w,
        None => return CDB_ERROR_OPERATION_FAILED,
    };

    let key = unsafe { slice::from_raw_parts(key_ptr, key_len) };
    let value = unsafe { slice::from_raw_parts(value_ptr, value_len) };

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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_writer_finalize(writer_ptr: *mut CdbWriterFile) -> c_int {
    if writer_ptr.is_null() {
        return CDB_ERROR_NULL_POINTER;
    }
    let writer_wrapper = unsafe { &mut *writer_ptr };
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_writer_free(writer_ptr: *mut CdbWriterFile) {
    if !writer_ptr.is_null() {
        unsafe { drop(Box::from_raw(writer_ptr)) };
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_open(path: *const c_char) -> *mut CdbFile {
    if path.is_null() {
        return ptr::null_mut();
    }
    let c_str = unsafe { CStr::from_ptr(path) };
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_get(
    reader_ptr: *mut CdbFile,
    key_ptr: *const c_uchar,
    key_len: size_t,
    value_out: *mut CdbData,
) -> c_int {
    if reader_ptr.is_null() || key_ptr.is_null() || value_out.is_null() {
        return CDB_ERROR_NULL_POINTER;
    }
    let reader_wrapper = unsafe { &mut *reader_ptr };
    let reader = match reader_wrapper.reader.as_mut() {
        Some(r) => r,
        None => return CDB_ERROR_OPERATION_FAILED,
    };
    let key = unsafe { slice::from_raw_parts(key_ptr, key_len) };

    match reader.get(key) {
        Ok(Some(value_vec)) => {
            let len = value_vec.len();
            let boxed_slice = value_vec.into_boxed_slice();
            unsafe {
                (*value_out).ptr = Box::into_raw(boxed_slice) as *const c_uchar;
                (*value_out).len = len;
            }
            CDB_SUCCESS
        }
        Ok(None) => {
            unsafe {
                (*value_out).ptr = ptr::null();
                (*value_out).len = 0;
            }
            CDB_SUCCESS
        }
        Err(e) => {
            eprintln!("Error in cdb_get: {}", e);
            unsafe {
                (*value_out).ptr = ptr::null();
                (*value_out).len = 0;
            }
            CDB_ERROR_IO
        }
    }
}

/// # Safety
///
/// `data.ptr` must be a pointer previously obtained from `cdb_get` that has not yet been freed.
/// `data.len` must be the length associated with that pointer.
/// If `data.ptr` is null, this function does nothing.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_free_data(data: CdbData) {
    if !data.ptr.is_null() {
        unsafe {
            drop(Box::from_raw(slice::from_raw_parts_mut(
                data.ptr as *mut u8,
                data.len,
            )))
        };
    }
}

/// # Safety
///
/// `reader_ptr` must be a valid pointer to a `CdbFile` obtained from `cdb_open`
/// or `ptr::null_mut()`. If it's a valid pointer, it must not be used after this function is called.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_close(reader_ptr: *mut CdbFile) {
    if !reader_ptr.is_null() {
        unsafe { drop(Box::from_raw(reader_ptr)) };
    }
}

// --- Iterator Implementation ---
// Iterator constants
pub const CDB_ITERATOR_HAS_NEXT: c_int = 1;
pub const CDB_ITERATOR_FINISHED: c_int = 0;

/// C-compatible structure for key-value pairs
/// Memory pointed to by key_ptr and value_ptr must be freed using cdb_free_data
#[repr(C)]
pub struct CdbKeyValue {
    key: CdbData,
    value: CdbData,
}

/// Owned iterator that manages CDB iteration without lifetime issues
/// This structure owns the CDB instance to avoid Rust lifetime complications in C FFI
pub struct OwnedCdbIterator {
    // Own the CDB to avoid lifetime issues
    cdb: Cdb<File, CdbHash>,
    // Iterator state - we'll manage this manually to avoid lifetime complications
    current_iterator: Option<cdb64::CdbIterator<'static, File, CdbHash>>,
}

impl OwnedCdbIterator {
    /// Create a new owned iterator
    /// This function uses unsafe code to work around lifetime issues
    fn new(cdb: Cdb<File, CdbHash>) -> Self {
        OwnedCdbIterator {
            cdb,
            current_iterator: None,
        }
    }

    /// Initialize the iterator (called on first next() call)
    fn ensure_iterator(&mut self) {
        if self.current_iterator.is_none() {
            // SAFETY: We extend the lifetime to 'static here
            // This is safe because:
            // 1. The iterator will only be used while this OwnedCdbIterator exists
            // 2. This OwnedCdbIterator owns the Cdb, so the Cdb will live as long as the iterator
            // 3. C code cannot outlive the OwnedCdbIterator due to our API design
            let cdb_ref: &'static Cdb<File, CdbHash> = unsafe { std::mem::transmute(&self.cdb) };
            self.current_iterator = Some(cdb_ref.iter());
        }
    }

    /// Get the next key-value pair
    #[allow(clippy::complexity)]
    fn next(&mut self) -> Option<Result<(Vec<u8>, Vec<u8>), std::io::Error>> {
        self.ensure_iterator();
        if let Some(ref mut iter) = self.current_iterator {
            iter.next()
        } else {
            None
        }
    }
}

/// Create a new iterator from a CdbFile
///
/// # Safety
///
/// `reader_ptr` must be a valid pointer to a `CdbFile` obtained from `cdb_open`.
/// The returned iterator must be freed with `cdb_iterator_free`.
/// After calling this function, `reader_ptr` should not be used directly as ownership
/// is transferred to the iterator.
///
/// # Returns
///
/// Returns a pointer to `OwnedCdbIterator` on success, null on failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_iterator_new(reader_ptr: *mut CdbFile) -> *mut OwnedCdbIterator {
    if reader_ptr.is_null() {
        return ptr::null_mut();
    }

    // Take ownership of the CdbFile
    let cdb_file = unsafe { Box::from_raw(reader_ptr) };

    // Extract the Cdb from CdbFile
    let cdb = match cdb_file.reader {
        Some(cdb) => cdb,
        None => return ptr::null_mut(),
    };

    Box::into_raw(Box::new(OwnedCdbIterator::new(cdb)))
}

/// Get the next key-value pair from the iterator
///
/// # Safety
///
/// `iter_ptr` must be a valid pointer to an `OwnedCdbIterator` obtained from `cdb_iterator_new`.
/// `kv_out` must point to a valid `CdbKeyValue` struct where the result will be stored.
/// If the function returns `CDB_ITERATOR_HAS_NEXT` (1), the memory pointed to by the pointers
/// in `kv_out` must be freed by calling `cdb_free_data`.
///
/// # Returns
///
/// - `CDB_ITERATOR_HAS_NEXT` (1) if there is a next key-value pair
/// - `CDB_ITERATOR_FINISHED` (0) if iteration is complete
/// - `CDB_ERROR_NULL_POINTER` (-1) if pointers are null
/// - `CDB_ERROR_IO` (-3) on I/O error
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_iterator_next(
    iter_ptr: *mut OwnedCdbIterator,
    kv_out: *mut CdbKeyValue,
) -> c_int {
    if iter_ptr.is_null() || kv_out.is_null() {
        return CDB_ERROR_NULL_POINTER;
    }

    let iterator = unsafe { &mut *iter_ptr };

    match iterator.next() {
        Some(Ok((key, value))) => {
            // Allocate memory for key
            let key_len = key.len();
            let key_boxed = key.into_boxed_slice();

            // Allocate memory for value
            let value_len = value.len();
            let value_boxed = value.into_boxed_slice();

            unsafe {
                (*kv_out).key.ptr = Box::into_raw(key_boxed) as *const c_uchar;
                (*kv_out).key.len = key_len;
                (*kv_out).value.ptr = Box::into_raw(value_boxed) as *const c_uchar;
                (*kv_out).value.len = value_len;
            }

            CDB_ITERATOR_HAS_NEXT
        }
        Some(Err(_)) => CDB_ERROR_IO,
        None => {
            // No more entries
            unsafe {
                (*kv_out).key.ptr = ptr::null();
                (*kv_out).key.len = 0;
                (*kv_out).value.ptr = ptr::null();
                (*kv_out).value.len = 0;
            }
            CDB_ITERATOR_FINISHED
        }
    }
}

/// Free an iterator and its associated resources
///
/// # Safety
///
/// `iter_ptr` must be a valid pointer to an `OwnedCdbIterator` obtained from `cdb_iterator_new`
/// or `ptr::null_mut()`. If it's a valid pointer, it must not be used after this function is called.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cdb_iterator_free(iter_ptr: *mut OwnedCdbIterator) {
    if !iter_ptr.is_null() {
        unsafe { drop(Box::from_raw(iter_ptr)) };
    }
}
