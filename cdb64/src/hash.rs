use std::hash::Hasher;

/// The initial seed value for the CDB hash function.
const CDB_HASH_START_VALUE: u64 = 5381;

/// Implements the CDB hash function.
///
/// This hash function is a variant of DJB hash (Daniel J. Bernstein).
/// It is used by CDB to distribute keys across hash tables.
///
/// # Examples
///
/// ```
/// use std::hash::Hasher;
/// use cdb64::CdbHash;
///
/// let mut hasher = CdbHash::new();
/// hasher.write(b"some data");
/// let hash_value = hasher.finish();
/// println!("Hash: {}", hash_value);
/// ```
#[derive(Clone)]
pub struct CdbHash {
    state: u64,
}

impl Default for CdbHash {
    fn default() -> Self {
        Self::new()
    }
}

impl CdbHash {
    /// Creates a new `CdbHash` instance, initialized with the CDB starting value.
    pub fn new() -> Self {
        CdbHash {
            state: CDB_HASH_START_VALUE,
        }
    }
}

impl Hasher for CdbHash {
    fn finish(&self) -> u64 {
        self.state
    }

    /// The hash state is updated for each byte in the input slice according to the formula:
    /// `hash = ((hash << 5) + hash) ^ byte` (using wrapping arithmetic).
    fn write(&mut self, bytes: &[u8]) {
        let mut val = self.state;
        for &byte in bytes {
            val = ((val << 5).wrapping_add(val)) ^ (byte as u64);
        }
        self.state = val;
    }

    /// This is a convenience method that converts the `u64` to its little-endian byte representation
    /// and then calls `write`.
    fn write_u64(&mut self, i: u64) {
        self.write(&i.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hasher;

    #[test]
    fn test_cdb_hash_new() {
        let hasher = CdbHash::new();
        assert_eq!(hasher.state, CDB_HASH_START_VALUE);
        assert_eq!(hasher.finish(), CDB_HASH_START_VALUE);
    }

    #[test]
    fn test_cdb_hash_write_single_byte() {
        let mut hasher = CdbHash::new();
        let byte = b'a';
        hasher.write(&[byte]);
        // ((5381 << 5) + 5381) ^ 'a'
        // (172192 + 5381) ^ 97
        // 177573 ^ 97 = 177508
        let expected_hash =
            ((CDB_HASH_START_VALUE << 5).wrapping_add(CDB_HASH_START_VALUE)) ^ (byte as u64);
        assert_eq!(hasher.finish(), expected_hash);
    }

    #[test]
    fn test_cdb_hash_write_multiple_bytes() {
        let mut hasher = CdbHash::new();
        let data = b"hello";
        hasher.write(data);

        let mut expected_state = CDB_HASH_START_VALUE;
        for &byte in data {
            expected_state = ((expected_state << 5).wrapping_add(expected_state)) ^ (byte as u64);
        }
        assert_eq!(hasher.finish(), expected_state);
    }

    #[test]
    fn test_cdb_hash_write_empty() {
        let mut hasher = CdbHash::new();
        hasher.write(b"");
        assert_eq!(hasher.finish(), CDB_HASH_START_VALUE);
    }

    #[test]
    fn test_cdb_hash_write_u64() {
        let mut hasher = CdbHash::new();
        let val: u64 = 0x123456789abcdef0;
        hasher.write_u64(val);

        let mut expected_hasher = CdbHash::new();
        expected_hasher.write(&val.to_le_bytes());

        assert_eq!(hasher.finish(), expected_hasher.finish());
    }

    #[test]
    fn test_cdb_hash_multiple_writes_cumulative() {
        let mut hasher1 = CdbHash::new();
        hasher1.write(b"hello");
        hasher1.write(b" ");
        hasher1.write(b"world");
        let hash1 = hasher1.finish();

        let mut hasher2 = CdbHash::new();
        hasher2.write(b"hello world");
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_cdb_hash_default_is_new() {
        let hasher_default: CdbHash = Default::default();
        let hasher_new = CdbHash::new();
        assert_eq!(hasher_default.finish(), hasher_new.finish());
    }
}
