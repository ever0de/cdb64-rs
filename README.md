# cdb64-rs

`cdb64` is a Rust implementation of Daniel J. Bernstein's cdb (constant database). CDB is a disk-based hash table that provides fast lookups and atomic updates. This library supports 64-bit file offsets, allowing for very large database files.

## Key Features

* **Fast Lookups**: O(1) average-case lookups with excellent cache locality
* **Atomic Updates**: Updates are performed atomically by replacing the database file
* **Efficient Space Usage**: Compact binary format with minimal overhead
* **64-bit Support**: Handle databases larger than 4GB with 64-bit offsets
* **Generic Hasher**: Supports any hash algorithm that implements the `std::hash::Hasher` trait. Defaults to CdbHash (Daniel J. Bernstein)
* **Multi-language Support**: Bindings for Node.js, Python, and C

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

## Mmap (Memory Mapping) Support

To use the mmap feature, you need to enable it in your `Cargo.toml`:

```toml
[dependencies]
cdb64 = { version = "*", features = ["mmap"] }
```

### Example: Opening a CDB with Mmap

```rust
use cdb64::{Cdb, CdbWriter, CdbHash};
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "my_database_mmap.cdb";

    // First, create a database file (same as before)
    let mut writer = CdbWriter::<File, CdbHash>::new(File::create(db_path)?)?;
    writer.put(b"key_mmap", b"value_from_mmap")?;
    writer.finalize()?;

    // Now, open the database using mmap
    // This requires the "mmap" feature to be enabled for cdb64
    #[cfg(feature = "mmap")]
    {
      let cdb_mmap = Cdb::<File, CdbHash>::open_mmap(db_path)?;

      if let Some(value) = cdb_mmap.get(b"key_mmap")? {
          println!("key_mmap: {}", String::from_utf8_lossy(&value));
          assert_eq!(value, b"value_from_mmap");
      }
    } 

    Ok(())
}
```

## Language Bindings

This library provides bindings for multiple programming languages, allowing you to use `cdb64` functionalities within your applications.

### Node.js

The Node.js binding is built using `napi-rs` and can be found in the `node/` directory.

For detailed instructions on building, installing, and using the Node.js bindings, please refer to the `README.md` file (if available) and the source code within the `node/` directory.

#### Quick Example (Node.js)

```javascript
import { CdbWriter, Cdb } from 'cdb64-node';
import fs from 'fs';
import path from 'path';
import os from 'os';

const dbPath = path.join(os.tmpdir(), 'test-node.cdb');

// Write
const writer = new CdbWriter(dbPath);
writer.put(Buffer.from('hello'), Buffer.from('node'));
writer.finalize();

// Read
const cdb = Cdb.open(dbPath);
const value = cdb.get(Buffer.from('hello'));
console.log(Buffer.from(value).toString()); // Output: node

// Iterate
for (const entry of cdb.iter()) {
  console.log(`Key: ${Buffer.from(entry.key).toString()}, Value: ${Buffer.from(entry.value).toString()}`);
}
```

### Python

The Python binding is built using `PyO3` and can be found in the `python/` directory.

For detailed instructions on building, installing, and using the Python bindings, please refer to the `README.md` file (if available) and the source code within the `python/` directory.

#### Quick Example (Python)

```python
import os
from cdb64_python import CdbWriter, Cdb

db_path = "test_python.cdb"

# Write
writer = CdbWriter(db_path)
writer.put(b"hello", b"python")
writer.finalize()

# Read
cdb = Cdb.open(db_path)
value = cdb.get(b"hello")
print(value.decode()) # Output: python

# Iterate
for key, value in cdb.iter():
  print(f"Key: {key.decode()}, Value: {value.decode()}")

```

### C

The C binding provides a native C API and can be found in the `c/` directory. The binding generates a dynamic library (`libcdb64_c.dylib` on macOS, `libcdb64_c.so` on Linux, `cdb64_c.dll` on Windows) and corresponding header files.

#### Building the C Library

```sh
cd c/
make
```

This will generate:

* Dynamic library: `target/{debug,release}/libcdb64_c.{dylib|so|dll}`
* Header file: `include/cdb64.h`

#### Quick Example (C)

```c
#include "cdb64.h"
#include <stdio.h>

int main() {
    // Write
    CdbWriterFile* writer = cdb_writer_create("test.cdb");
    if (writer == NULL) {
        fprintf(stderr, "Failed to create writer\n");
        return 1;
    }
    
    int ret = cdb_writer_put(writer, (unsigned char*)"hello", 5, (unsigned char*)"world", 5);
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to put data\n");
        cdb_writer_free(writer);
        return 1;
    }
    
    ret = cdb_writer_finalize(writer);
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to finalize writer\n");
        cdb_writer_free(writer);
        return 1;
    }
    cdb_writer_free(writer);
    
    // Read
    CdbFile* cdb = cdb_open("test.cdb");
    if (cdb == NULL) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }
    
    CdbData result;
    if (cdb_get(cdb, (unsigned char*)"hello", 5, &result) == CDB_SUCCESS && result.ptr) {
        printf("Value: %.*s\n", (int)result.len, result.ptr);
        cdb_free_data(result);
    }
    
    // Iterate
    OwnedCdbIterator* iter = cdb_iterator_new(cdb); // Note: cdb is consumed
    CdbKeyValue kv;
    while (cdb_iterator_next(iter, &kv) == CDB_ITERATOR_HAS_NEXT) {
        printf("Key: %.*s, Value: %.*s\n", 
               (int)kv.key.len, kv.key.ptr, (int)kv.value.len, kv.value.ptr);
        cdb_free_data(kv.key);
        cdb_free_data(kv.value);
    }
    cdb_iterator_free(iter);
    return 0;
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

Benchmarks are run using Criterion.rs. The following results were obtained on a machine with `Apple M2 Pro` and `32GB RAM` running `Sequoia 15.4.1(24E263)`. All benchmarks use `CdbHash` and operate on a dataset of 10,000 key-value pairs, where keys are strings like "key0", "key1", ..., and values are random byte arrays of 10-200 bytes.

* **Write Performance (to temporary file)**: `CdbWriter/write_temp_file`
  * Time: ~105.70 ms
* **Write Performance (to in-memory buffer)**: `CdbWriter/write_in_memory`
  * Time: ~452.24 µs
* **Read Performance (from file, uncached)**: `CdbReader/get_from_file_uncached`
  * Description: Reads from a cdb file, re-opening the file for each batch to minimize OS caching effects. Represents a "cold read" scenario.
  * Time: ~17.73 ms
* **Read Performance (from file, cached)**: `CdbReader/get_from_file_cached`
  * Description: Reads from an already open cdb file. Represents a "warm read" scenario where parts of the file might be cached by the OS.
  * Time: ~17.67 ms
* **Read Performance (from in-memory buffer)**: `CdbReader/get_from_memory`
  * Description: Reads from a cdb structure entirely in memory.
  * Time: ~900.18 ms (for 10,000 lookups)
* **Read Performance (from file with mmap, uncached)**: `CdbReader/get_from_file_mmap_uncached`
  * Description: Reads from a cdb file using memory-mapping, re-opening and re-mapping the file for each batch.
  * Time: ~1.00 ms (for 10,000 lookups)
* **Read Performance (from file with mmap, cached)**: `CdbReader/get_from_file_mmap_cached`
  * Description: Reads from an already open and memory-mapped cdb file.
  * Time: ~920.98 µs (for 10,000 lookups)

*(Note: The `get_from_memory` benchmark involves a loop of 10,000 `get` operations. The reported time is for the entire loop.)*

To run the benchmarks yourself:

```sh
cargo bench
# To include mmap benchmarks
cargo bench --features mmap
```

The results will be available in `target/criterion/report/index.html`.

## License

MIT. See [LICENSE](./LICENSE) for details.  
Original cdb64 (Go) by Colin Marc, CDB64 by Chris Lu. This is a Rust port.
