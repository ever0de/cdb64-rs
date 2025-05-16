# cdb64-rs

`cdb64` is a Rust implementation of Daniel J. Bernstein's cdb (constant database). CDB is a disk-based hash table that provides fast lookups and atomic updates. This library supports 64-bit file offsets, allowing for very large database files.

## Key Features

* **Fast Lookups**: Retrieving values for keys is very fast.
* **Atomic Updates**: Updates are performed atomically by replacing the database file.
* **Efficient Space Usage**: The database structure is compact.
* **Generic Hasher**: Supports any hash algorithm that implements the `std::hash::Hasher` trait. Defaults to SipHash 1-3.

## Usage

### Add Dependency

Add the following to your `Cargo.toml` file:

```toml
[dependencies]
cdb64 = "*" # Please adjust to the actual version published on crates.io
```

### Simple Example: Writing and Reading Data with Files

```rust
use cdb64::{Cdb, CdbWriter};
use std::fs::File;
use std::io::{Read, Seek, Write}; 
use std::collections::hash_map::DefaultHasher; 

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Write example
    let mut writer = CdbWriter::<_, DefaultHasher>::new(File::create("my_database.cdb")?)?;
    writer.put(b"key1", b"value1")?;
    writer.put(b"key2", b"value2")?;
    writer.put(b"another_key", b"another_value")?;
    writer.finalize()?; // Commit changes to disk

    // Read example
    let cdb = Cdb::<_, DefaultHasher>::open("my_database.cdb")?;

    // Get a value
    if let Some(value) = cdb.get(b"key1")? {
        println!("key1: {}", String::from_utf8_lossy(&value));
    }

    if let Some(value) = cdb.get(b"key2")? {
        println!("key2: {}", String::from_utf8_lossy(&value));
    }

    // Non-existent key
    assert!(cdb.get(b"non_existent_key")?.is_none());

    Ok(())
}
```

## How it Works

A cdb file consists of three parts:

1. **Fixed-size Header**: Contains a list of 256 hash table pointers. Each pointer points to a (hash value, file offset) pair.
2. **Data Records**: A sequence of consecutive key-value pairs. Each record is in the format (key length, value length, key, value).
3. **Hash Tables**: Located at the end of the file, consisting of 256 hash tables, each a list of (hash value, record offset) pairs.

When looking up a key, cdb follows these steps:

1. Hashes the key to compute a hash value.
2. Uses the lower bits of the hash value to select one of the 256 hash tables.
3. Searches for a matching hash value within the selected hash table.
4. If a matching hash value is found, it moves to the corresponding record offset and compares the key.
5. If the key matches, it returns the corresponding value.

This structure is designed to allow lookups with an average of just two disk accesses.

## Original CDB Specification

For more detailed information, please refer to Daniel J. Bernstein's [cdb specification](https://cr.yp.to/cdb.html).

## Benchmarks

Benchmarks are run using Criterion.rs. The following results were obtained on a machine with `Apple M2 Pro` and `32GB RAM` running `Sequoia 15.4.1(24E263)`. All benchmarks use `CdbHash` (based on SipHash 1-3) and operate on a dataset of 10,000 key-value pairs, where keys are strings like "key0", "key1", ..., and values are random byte arrays of 10-200 bytes.

* **Write Performance (to temporary file)**: `CdbWriter/write_temp_file`
  * Time: ~103 ms
* **Write Performance (to in-memory buffer)**: `CdbWriter/write_in_memory`
  * Time: ~500 µs
* **Read Performance (from file, uncached)**: `CdbReader/get_from_file_uncached`
  * Description: Reads from a cdb file, re-opening the file for each batch to minimize OS caching effects. Represents a "cold read" scenario.
  * Time: ~18 ms
* **Read Performance (from file, cached)**: `CdbReader/get_from_file_cached`
  * Description: Reads from an already open cdb file. Represents a "warm read" scenario where parts of the file might be cached by the OS.
  * Time: ~17.6 ms
* **Read Performance (from in-memory buffer)**: `CdbReader/get_from_memory`
  * Description: Reads from a cdb structure entirely in memory.
  * Time: ~3.8 s (for 10,000 lookups)

*(Note: The `get_from_memory` benchmark involves a loop of 10,000 `get` operations. The reported time is for the entire loop.)*

To run the benchmarks yourself:

```sh
cargo bench
```

The results will be available in `target/criterion/report/index.html`.

## License

This project is licensed under either of

* MIT license
* Apache License, Version 2.0
