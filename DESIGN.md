# cdb64-rs Design Document

This document outlines the internal design of the `cdb64-rs` library, a Rust implementation of a constant database (CDB) with 64-bit offset support.

## 1. CDB File Structure

A cdb64 file is composed of three main sections, laid out sequentially:

1.  **Header**: A fixed-size section at the beginning of the file.
2.  **Data Records**: Variable-length records containing key-value pairs.
3.  **Hash Tables**: A collection of tables used to quickly locate data records.

```ascii
+-----------------------------------------+
| Header (256 entries, each 16 bytes)     |  Fixed size: 256 * (8-byte pos + 8-byte len) = 4096 bytes
| - Pointer to Hash Table 0 (pos, len)    |
| - Pointer to Hash Table 1 (pos, len)    |
| ...                                     |
| - Pointer to Hash Table 255 (pos, len)  |
+-----------------------------------------+
| Data Record 1                           |  (key_len, value_len, key_bytes, value_bytes)
|  - Key Length (u32)                     |
|  - Value Length (u32)                   |
|  - Key Bytes                            |
|  - Value Bytes                          |
+-----------------------------------------+
| Data Record 2                           |
|  - Key Length (u32)                     |
|  - Value Length (u32)                   |
|  - Key Bytes                            |
|  - Value Bytes                          |
+-----------------------------------------+
| ... (more data records)                 |
+-----------------------------------------+
| Hash Table 0                            |  Array of slots (hash_value_high_bits, record_offset)
|  - Slot 1 (u64 hash, u64 offset)        |
|  - Slot 2 (u64 hash, u64 offset)        |
|  ...                                     |
+-----------------------------------------+
| Hash Table 1                            |
|  - Slot 1 (u64 hash, u64 offset)        |
|  - Slot 2 (u64 hash, u64 offset)        |
|  ...                                     |
+-----------------------------------------+
| ... (more hash tables, up to 255)       |
+-----------------------------------------+
```

### 1.1. Header

*   Size: 4096 bytes (256 entries * 16 bytes/entry).
*   Each entry is a pair of `u64` values:
    *   `table_pos`: The starting file offset of the corresponding hash table.
    *   `table_len`: The number of slots in that hash table.
*   The `i`-th entry in the header corresponds to `Hash Table i`.

### 1.2. Data Records

*   Data records are stored sequentially after the header.
*   Each record consists of:
    1.  `key_len` (u32): Length of the key in bytes.
    2.  `value_len` (u32): Length of the value in bytes.
    3.  `key`: The key itself (byte array).
    4.  `value`: The value itself (byte array).
*   The file offset of a data record is crucial for lookups.
*   The (key_len, value_len) pair now occupies 8 bytes (4 bytes each).

The total size of a data record is `4 (key_len) + 4 (value_len) + key_len + value_len` bytes.

### 1.3. Hash Tables

*   There are 256 hash tables, stored sequentially after all data records.
*   Each hash table is an array of "slots".
*   Each slot is a pair of `u64` values:
    *   `hash_value_high_bits`: The higher bits of the full hash of the key (used for collision resolution within the table).
    *   `record_offset`: The file offset where the actual data record (key-value pair) begins.
*   A slot with `(0, 0)` typically indicates an empty or end-of-table marker, though the original cdb specification uses `(0,0)` to mark the end of a chain in open addressing, which is slightly different here as we store tables contiguously. In this implementation, the length from the header determines the table bounds.

## 2. Write Process (`CdbWriter`)

The `CdbWriter` is responsible for creating a new cdb64 file.

```ascii
User Calls:
writer.put(key1, val1)
writer.put(key2, val2)
...
writer.finalize()

Internal Steps:

1. Initial State:
   - `writer_impl`: Underlying writer (e.g., File, Cursor<Vec<u8>>)
   - `records`: Vec<(key_hash, key_bytes, value_bytes)> - Temporary store for records
   - `current_pos`: Tracks current write position for data records (starts after header space)

2. For each `put(key, value)`:
   - Calculate `hash = H::hash(key)`.
   - Store `(hash, key, value)` in `records`.
   - `current_pos` is NOT updated yet for data records. Data is written during `finalize`.

3. `finalize()`:
   a. Reserve Header Space:
      - If writing to a seekable stream, seek past the header (4096 bytes).
      - `data_records_start_pos = current_pos` (which is initially 0 or header_size if pre-allocated).

   b. Write Data Records:
      - Iterate through `records` collected in step 2.
      - For each `(hash, key, value)`:
         - Write `key.len()` (u32).
         - Write `value.len()` (u32).
         - Write `key` bytes.
         - Write `value` bytes.
         - Store the `record_offset` (current file position before writing this record).
         - Add `(hash, record_offset)` to a temporary list for building hash tables. Let's call this `hash_pointers`.
      - `hash_tables_start_pos = current_pos` after all data records are written.

   c. Prepare Hash Tables:
      - Create 256 empty lists, `hash_table_slots[0...255]`.
      - For each `(full_hash, record_offset)` in `hash_pointers`:
         - `table_index = full_hash % 256`.
         - `hash_high_bits = full_hash / 256` (or other consistent derivation of high bits).
         - Add `(hash_high_bits, record_offset)` to `hash_table_slots[table_index]`.

   d. Write Hash Tables:
      - `header_entries`: Vec<(table_pos, table_len)> to store header data.
      - For `i` from 0 to 255:
         - `current_table_pos = current_pos` (file offset where this hash table starts).
         - `slots_for_table_i = hash_table_slots[i]`.
         - Sort `slots_for_table_i` by `hash_high_bits` (optional but good for lookup performance if linear scan is long).
         - For each `(h_high, rec_off)` in `slots_for_table_i`:
            - Write `h_high` (u64).
            - Write `rec_off` (u64).
         - Add `(current_table_pos, slots_for_table_i.len())` to `header_entries`.
         - `current_pos` is updated after writing each table.

   e. Write Header:
      - If seekable, seek to the beginning of the file (position 0).
      - For `i` from 0 to 255:
         - `(pos, len) = header_entries[i]`.
         - Write `pos` (u64).
         - Write `len` (u64).

   f. Flush the writer.
```

Key points:
*   Data is written first, then hash tables, then the header is backfilled. This requires a seekable writer or a two-pass approach if the writer is not seekable (e.g., writing to a temporary buffer first, then to the final stream). `cdb64-rs` currently assumes a seekable writer.
*   The `current_pos` variable in `CdbWriter` tracks the end of the data section, which becomes the start of the hash table section.

## 3. Read Process (`Cdb`)

The `Cdb` struct provides methods to read data from an existing cdb64 file.

```ascii
User Calls:
cdb.get(search_key)

Internal Steps:

1. Open File:
   - `Cdb::open(path)` memory-maps the file or opens it for buffered reading.
   - Reads the 256 header entries into memory (`header_pointers`).

2. `get(search_key)`:
   a. Calculate Hash:
      - `full_hash = H::hash(search_key)`.

   b. Locate Hash Table:
      - `table_idx = full_hash % 256`.
      - `(table_pos, table_len) = header_pointers[table_idx]`.
      - If `table_len == 0`, key is not found.

   c. Search Hash Table:
      - `hash_to_match = full_hash / 256` (must match derivation in writer).
      - Seek to `table_pos` in the file.
      - Iterate `table_len` times (for each slot in the identified hash table):
         - Read `slot_hash_high_bits` (u64).
         - Read `slot_record_offset` (u64).
         - If `slot_hash_high_bits == hash_to_match`:
            - This is a potential match. Proceed to key comparison.
            - Seek to `slot_record_offset`.
            - Read `record_key_len` (u32).
            - Read `record_value_len` (u32).
            - If `record_key_len == search_key.len()`:
               - Read `record_key_bytes` (of `record_key_len`).
               - If `record_key_bytes == search_key`:
                  - Key found! The value starts at `slot_record_offset + 4 (key_len_size) + 4 (val_len_size) + record_key_len`.
                  - Return a reader or the value itself (e.g., `Some(Vec<u8>)` or `Some(Box<dyn Read + Seek>)`).
      - If loop finishes without a match, key is not found. Return `None`.
```

Key points:
*   A lookup typically involves:
    1.  One read for the header (done at `Cdb::open` and cached).
    2.  One seek and read for the relevant hash table slots.
    3.  One or more seeks and reads for data records if hash collisions occur (i.e., multiple keys map to the same `hash_to_match` within the same table).
*   The efficiency comes from the small number of disk accesses required on average.

## 4. Iteration Process (`CdbIterator`)

The `CdbIterator` enables sequential iteration over all key-value pairs in the database using Rust's standard `Iterator` trait.

```ascii
User Calls:
let mut iter = cdb.iter()?;
while let Some(result) = iter.next() {
    match result {
        Ok((key, value)) => {
            // process key, value (both are Vec<u8>)
        }
        Err(e) => {
            // handle error
        }
    }
}

Internal Steps:

1. `Cdb::iter()`:
   - Creates a `CdbIterator`.
   - `current_pos` is initialized to the start of the data records section (i.e., `HEADER_SIZE = 4096`).
   - `end_pos` is determined by finding the minimum `table_pos` from the header. This marks where the first hash table begins, and thus where data records end. If all hash tables are empty, `end_pos = HEADER_SIZE` (no data records).

2. `CdbIterator`'s `next()`:
   a. End of iteration:
      - If `current_pos >= end_pos`, iteration is complete and returns `None`.

   b. Reading a record:
      - Reads the key and value lengths (`u32`, `u32`) at `current_pos`.
      - Reads the key and value bytes into `Vec<u8>`.
      - Advances `current_pos` by the total record length (`8 + key_len + value_len`).
      - If the record would extend past `end_pos`, returns `Err(io::Error)`.
      - On success, returns `Some(Ok((key, value)))`.
      - On read error, returns `Some(Err(e))`.
```

Key points:

- The iterator uses the standard Rust `Iterator` trait (`next()` method).
- Each iteration yields a `(Vec<u8>, Vec<u8>)` pair (key and value), or an error (`io::Error`).
- Records are read sequentially from the data section, which ends at the start of the first hash table.
- Returns `None` when iteration is complete or there are no records.

## 5. Hasher

*   The library is generic over `std::hash::Hasher + Default`.
*   The same hasher (and its specific algorithm, e.g., SipHash 1-3 via `DefaultHasher`) must be used for both writing and reading a CDB file for lookups to be successful.
*   The full 64-bit hash output is used:
    *   Lower bits (e.g., `hash % 256`) select the hash table.
    *   Remaining higher bits (e.g., `hash / 256`) are stored in the hash table slots to disambiguate entries within that table.

This design aims to be consistent with the principles of DJB's CDB while extending it for 64-bit offsets and providing a flexible Rust API.
