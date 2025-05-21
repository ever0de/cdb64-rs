use cdb64::{Cdb, CdbHash, CdbWriter};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{fs::File, io::Cursor};
use tempfile::NamedTempFile;

const NUM_ENTRIES_FOR_BENCH: usize = 10_000; // Number of entries for benchmark

fn generate_kv_pairs(count: usize, seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|i| {
            let key = format!("key{}", i).into_bytes();
            let value_len = rng.random_range(10..200);
            let value = (0..value_len)
                .map(|_| rng.random::<u8>())
                .collect::<Vec<u8>>();
            (key, value)
        })
        .collect()
}

fn cdb_write_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("CdbWriter");
    let data = generate_kv_pairs(NUM_ENTRIES_FOR_BENCH, 42);

    group.bench_function("write_temp_file", |b| {
        b.iter(|| {
            let temp_file = NamedTempFile::new().unwrap();
            let mut writer =
                CdbWriter::<_, CdbHash>::new(File::create(temp_file.path()).unwrap()).unwrap();
            for (key, value) in data.iter() {
                writer
                    .put(std::hint::black_box(key), std::hint::black_box(value))
                    .unwrap();
            }
            writer.finalize().unwrap();
            // temp_file is dropped and deleted here
        })
    });

    group.bench_function("write_in_memory", |b| {
        b.iter(|| {
            let mut writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new())).unwrap();
            for (key, value) in data.iter() {
                writer
                    .put(std::hint::black_box(key), std::hint::black_box(value))
                    .unwrap();
            }
            writer.finalize().unwrap();
        })
    });
    group.finish();
}

fn cdb_read_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("CdbReader");
    let data = generate_kv_pairs(NUM_ENTRIES_FOR_BENCH, 42);
    let keys_to_lookup: Vec<Vec<u8>> = data.iter().map(|(k, _)| k.clone()).collect();

    // Create file-based CDB
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_path_buf();
    let mut writer_file = CdbWriter::<_, CdbHash>::new(File::create(&file_path).unwrap()).unwrap();
    for (key, value) in data.iter() {
        writer_file.put(key, value).unwrap();
    }
    writer_file.finalize().unwrap();

    // Create in-memory CDB
    let mut mem_writer = CdbWriter::<_, CdbHash>::new(Cursor::new(Vec::new())).unwrap();
    for (key, value) in data.iter() {
        mem_writer.put(key, value).unwrap();
    }
    mem_writer.finalize().unwrap();
    let cdb_vec_data = mem_writer.into_inner().unwrap().into_inner();

    group.bench_function("get_from_file_uncached", |b| {
        b.iter_batched(
            || {
                // Re-open the file in each iteration to minimize OS caching effects
                Cdb::<_, CdbHash>::open(&file_path).unwrap()
            },
            |cdb| {
                for key in keys_to_lookup.iter() {
                    if let Some(value_bytes) = cdb.get(std::hint::black_box(key)).unwrap() {
                        std::hint::black_box(value_bytes);
                    }
                }
                // cdb is dropped here
            },
            criterion::BatchSize::SmallInput, // Setup cost is high per iteration
        )
    });

    {
        let cdb_file_cached = Cdb::<_, CdbHash>::open(&file_path).unwrap();
        group.bench_function("get_from_file_cached", |b| {
            b.iter(|| {
                for key in keys_to_lookup.iter() {
                    if let Some(value_bytes) =
                        cdb_file_cached.get(std::hint::black_box(key)).unwrap()
                    {
                        std::hint::black_box(value_bytes);
                    }
                }
            })
        });
    }

    // Pre-create the in-memory Cdb instance
    let cdb_in_memory = Cdb::<_, CdbHash>::new(Cursor::new(cdb_vec_data)).unwrap();

    group.bench_function("get_from_memory", |b| {
        b.iter(|| {
            for key in keys_to_lookup.iter() {
                if let Some(value_bytes) = cdb_in_memory.get(std::hint::black_box(key)).unwrap() {
                    std::hint::black_box(value_bytes);
                }
            }
        })
    });

    #[cfg(feature = "mmap")]
    {
        // Benchmark for mmap access (uncached, re-opening file)
        group.bench_function("get_from_file_mmap_uncached", |b| {
            b.iter_batched(
                || {
                    // Re-open the file and mmap in each iteration
                    Cdb::<File, CdbHash>::open_mmap(&file_path).unwrap()
                },
                |cdb_mmap| {
                    for key in keys_to_lookup.iter() {
                        if let Some(value_bytes) = cdb_mmap.get(std::hint::black_box(key)).unwrap()
                        {
                            std::hint::black_box(value_bytes);
                        }
                    }
                    // cdb_mmap is dropped here
                },
                criterion::BatchSize::SmallInput, // Setup cost is high
            )
        });

        // Benchmark for mmap access (cached, file opened once)
        let cdb_file_mmap_cached = Cdb::<File, CdbHash>::open_mmap(&file_path).unwrap();
        group.bench_function("get_from_file_mmap_cached", |b| {
            b.iter(|| {
                for key in keys_to_lookup.iter() {
                    if let Some(value_bytes) =
                        cdb_file_mmap_cached.get(std::hint::black_box(key)).unwrap()
                    {
                        std::hint::black_box(value_bytes);
                    }
                }
            })
        });
    }

    group.finish();
}

criterion_group!(benches, cdb_write_benchmark, cdb_read_benchmark);
criterion_main!(benches);
