use std::io::{Error, ErrorKind, Result, Write};

/// A trait for objects that can be read from at a specific offset.
/// Similar to Go's `io.ReaderAt`.
pub trait ReaderAt {
    /// Reads up to `buf.len()` bytes into `buf` starting at `offset`.
    /// Returns the number of bytes read.
    /// This method does not affect the current cursor position of the reader if it has one.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Reads exactly `buf.len()` bytes into `buf` starting at `offset`.
    /// If EOF is reached before `buf` is filled, an error of kind `ErrorKind::UnexpectedEof` is returned.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer in read_exact_at",
                    ));
                }
                Ok(n) => {
                    let tmp = buf; // Necessary due to borrow checker limitations with re-slicing buf in place
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {} // Retry on interrupt
                Err(e) => return Err(e),                           // Other errors
            }
        }
        Ok(())
    }
}

/// Implement `ReaderAt` for `std::fs::File` on Unix-like systems.
#[cfg(unix)]
impl ReaderAt for std::fs::File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        FileExt::read_at(self, buf, offset)
    }
}

/// Implement `ReaderAt` for byte slices, useful for testing or in-memory data.
impl ReaderAt for &'_ [u8] {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let offset_usize = offset as usize;

        // Check if offset is beyond the length of the slice
        if offset_usize >= self.len() {
            return Ok(0); // EOF, no bytes read
        }

        let remaining_in_self = self.len() - offset_usize;
        let bytes_to_copy = std::cmp::min(buf.len(), remaining_in_self);

        if bytes_to_copy > 0 {
            buf[..bytes_to_copy].copy_from_slice(&self[offset_usize..offset_usize + bytes_to_copy]);
        }
        Ok(bytes_to_copy)
    }
}

/// Implement `ReaderAt` for `std::io::Cursor<Vec<u8>>`.
impl ReaderAt for std::io::Cursor<Vec<u8>> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::io::{Read, Seek, SeekFrom};
        let mut inner_cursor = self.clone(); // Clone to avoid affecting the original cursor's position
        inner_cursor.seek(SeekFrom::Start(offset))?;
        inner_cursor.read(buf)
    }
}

/// Reads two u32 values (a tuple) from a `ReaderAt` at the given offset.
/// The values are expected to be encoded in little-endian format, 4 bytes each.
pub fn read_tuple<R: ReaderAt + ?Sized>(reader: &R, offset: u64) -> Result<(u32, u32)> {
    let mut buffer = [0u8; 8]; // Buffer for two u32 values
    reader.read_exact_at(&mut buffer, offset)?;

    // Safely convert parts of the buffer to u32.
    // These try_into calls should not fail if read_exact_at succeeded with an 8-byte buffer.
    let first_bytes: [u8; 4] = buffer[0..4].try_into().map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "Internal error: Failed to slice buffer for first u32",
        )
    })?;
    let second_bytes: [u8; 4] = buffer[4..8].try_into().map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "Internal error: Failed to slice buffer for second u32",
        )
    })?;

    let first = u32::from_le_bytes(first_bytes);
    let second = u32::from_le_bytes(second_bytes);
    Ok((first, second))
}

/// Writes two u32 values (a tuple) to a `Write` stream.
/// The values are encoded in little-endian format, 4 bytes each.
pub fn write_tuple<W: Write + ?Sized>(writer: &mut W, first: u32, second: u32) -> Result<()> {
    writer.write_all(&first.to_le_bytes())?;
    writer.write_all(&second.to_le_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*; // Import items from the parent module
    use std::io::Cursor;

    // Tests for ReaderAt on &[u8]
    #[test]
    fn test_reader_at_slice_read_full() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(buf, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_reader_at_slice_read_partial() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        let mut buf = [0u8; 3];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 3);
        assert_eq!(buf, [1, 2, 3]);
    }

    #[test]
    fn test_reader_at_slice_read_offset() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        let mut buf = [0u8; 3];
        let n = data.read_at(&mut buf, 2).unwrap();
        assert_eq!(n, 3);
        assert_eq!(buf, [3, 4, 5]);
    }

    #[test]
    fn test_reader_at_slice_read_offset_partial() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        let mut buf = [0u8; 5]; // buffer larger than remaining data from offset
        let n = data.read_at(&mut buf, 3).unwrap();
        assert_eq!(n, 2);
        assert_eq!(buf[0..2], [4, 5]);
    }

    #[test]
    fn test_reader_at_slice_read_empty_buf() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        let mut buf = [0u8; 0];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_reader_at_slice_read_at_eof() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        let mut buf = [0u8; 3];
        let n = data.read_at(&mut buf, 5).unwrap();
        assert_eq!(n, 0); // EOF, should read 0 bytes
    }

    #[test]
    fn test_reader_at_slice_read_past_eof() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        let mut buf = [0u8; 3];
        let n = data.read_at(&mut buf, 10).unwrap();
        assert_eq!(n, 0); // Past EOF, should read 0 bytes
    }

    // Tests for read_exact_at on &[u8]
    #[test]
    fn test_read_exact_at_slice_success() {
        let data: &[u8] = &[10, 20, 30, 40, 50];
        let mut buf = [0u8; 3];
        data.read_exact_at(&mut buf, 1).unwrap();
        assert_eq!(buf, [20, 30, 40]);
    }

    #[test]
    fn test_read_exact_at_slice_eof() {
        let data: &[u8] = &[10, 20, 30];
        let mut buf = [0u8; 4];
        let result = data.read_exact_at(&mut buf, 0);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_read_exact_at_slice_eof_at_offset() {
        let data: &[u8] = &[10, 20, 30, 40, 50];
        let mut buf = [0u8; 3];
        let result = data.read_exact_at(&mut buf, 3);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), ErrorKind::UnexpectedEof);
    }

    // Tests for read_tuple
    #[test]
    fn test_read_tuple_success() {
        let val1: u32 = 0x05060708;
        let val2: u32 = 0x0D0E0F10;
        let mut bytes_vec = Vec::new();
        bytes_vec.extend_from_slice(&val1.to_le_bytes());
        bytes_vec.extend_from_slice(&val2.to_le_bytes());
        bytes_vec.extend_from_slice(&[1, 2, 3]); // Extra data
        let bytes_slice = &bytes_vec[..];

        let (r_val1, r_val2) = read_tuple(&bytes_slice, 0).unwrap();
        assert_eq!(r_val1, val1);
        assert_eq!(r_val2, val2);

        // Test with offset
        let val3: u32 = 0x15161718;
        let val4: u32 = 0x1D1E1F20;
        let mut bytes_offset_vec = Vec::new();
        bytes_offset_vec.extend_from_slice(&[0xFF, 0xFE]); // Prefix
        bytes_offset_vec.extend_from_slice(&val3.to_le_bytes());
        bytes_offset_vec.extend_from_slice(&val4.to_le_bytes());
        let bytes_offset_slice = &bytes_offset_vec[..];

        let (r_val3, r_val4) = read_tuple(&bytes_offset_slice, 2).unwrap();
        assert_eq!(r_val3, val3);
        assert_eq!(r_val4, val4);
    }

    #[test]
    fn test_read_tuple_eof() {
        let val1: u32 = 0x01020304;
        let mut bytes_vec = Vec::new();
        bytes_vec.extend_from_slice(&val1.to_le_bytes());
        // Missing the second u32
        let bytes_slice = &bytes_vec[..];

        let result = read_tuple(&bytes_slice, 0);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), ErrorKind::UnexpectedEof);

        // Not enough bytes for even one u32
        let short_bytes_slice: &[u8] = &[1, 2, 3];
        let result_short = read_tuple(&short_bytes_slice, 0);
        assert!(result_short.is_err());
        assert_eq!(result_short.err().unwrap().kind(), ErrorKind::UnexpectedEof);
    }

    // Tests for write_tuple
    #[test]
    fn test_write_tuple_success() {
        let val1: u32 = 0xA5A6A7A8;
        let val2: u32 = 0xB5B6B7B8;
        let mut buffer = Cursor::new(Vec::new());

        write_tuple(&mut buffer, val1, val2).unwrap();

        let written_bytes = buffer.into_inner();
        assert_eq!(written_bytes.len(), 8); // 4 bytes for each u32

        let mut expected_bytes = Vec::new();
        expected_bytes.extend_from_slice(&val1.to_le_bytes());
        expected_bytes.extend_from_slice(&val2.to_le_bytes());

        assert_eq!(written_bytes, expected_bytes);
    }

    // Mock ReaderAt that can simulate errors for testing read_exact_at error paths
    use std::cell::Cell;
    struct MockReaderAtWithCount {
        data: Vec<u8>,
        read_limit: Option<usize>,
        fail_on_nth_read: Option<usize>,
        read_count: Cell<usize>,
    }

    impl ReaderAt for MockReaderAtWithCount {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            let current_read_count = self.read_count.get();
            self.read_count.set(current_read_count + 1);

            if let Some(fail_at) = self.fail_on_nth_read {
                if self.read_count.get() == fail_at {
                    return Err(std::io::Error::other("Simulated read error"));
                }
            }

            let offset_usize = offset as usize;
            if offset_usize >= self.data.len() {
                return Ok(0);
            }

            let mut bytes_to_copy = std::cmp::min(buf.len(), self.data.len() - offset_usize);
            if let Some(limit) = self.read_limit {
                bytes_to_copy = std::cmp::min(bytes_to_copy, limit);
            }

            if bytes_to_copy > 0 {
                buf[..bytes_to_copy]
                    .copy_from_slice(&self.data[offset_usize..offset_usize + bytes_to_copy]);
            }
            Ok(bytes_to_copy)
        }
    }

    #[test]
    fn test_read_exact_at_with_partial_reads() {
        let reader = MockReaderAtWithCount {
            data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            read_limit: Some(2), // Simulate a reader that only returns 2 bytes at a time
            fail_on_nth_read: None,
            read_count: Cell::new(0),
        };
        let mut buf = [0u8; 5];
        reader.read_exact_at(&mut buf, 1).unwrap();
        assert_eq!(buf, [2, 3, 4, 5, 6]);
        assert_eq!(reader.read_count.get(), 3); // 2 bytes, 2 bytes, 1 byte
    }

    #[test]
    fn test_read_exact_at_error_after_partial_read() {
        let reader = MockReaderAtWithCount {
            data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            read_limit: Some(3),
            fail_on_nth_read: Some(2), // Fail on the second read_at call
            read_count: Cell::new(0),
        };
        let mut buf = [0u8; 5];
        let result = reader.read_exact_at(&mut buf, 0);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), ErrorKind::Other);
        assert_eq!(buf[0..3], [1, 2, 3]);
        assert_eq!(reader.read_count.get(), 2);
    }
}
