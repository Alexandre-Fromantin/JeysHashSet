use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

use rand::{TryRng, rngs::SysRng};

fn x3_hash_64_to_64(input: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(input)
}

fn x64_hash_64_to_64(input: &[u8], seed: u64) -> u64 {
    xxhash_rust::xxh64::xxh64(input, seed)
}

fn x32_hash_64_to_64(input: &[u8], seed: u32) -> u32 {
    xxhash_rust::xxh32::xxh32(input, seed)
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("x3_hash_64_to_64", |b| {
        b.iter_batched(
            || SysRng.try_next_u64().unwrap().to_be_bytes(),
            |data| x3_hash_64_to_64(&data),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("x64_hash_64_to_32", |b| {
        b.iter_batched(
            || {
                (
                    SysRng.try_next_u64().unwrap().to_be_bytes(),
                    SysRng.try_next_u64().unwrap(),
                )
            },
            |(input, seed)| x64_hash_64_to_64(&input, seed),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("x32_hash_64_to_32", |b| {
        b.iter_batched(
            || {
                (
                    SysRng.try_next_u64().unwrap().to_be_bytes(),
                    SysRng.try_next_u32().unwrap(),
                )
            },
            |(input, seed)| x32_hash_64_to_64(&input, seed),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
