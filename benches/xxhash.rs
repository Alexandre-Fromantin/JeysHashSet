use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

use rand::{TryRng, rngs::SysRng};

fn xxhash3_64(input: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(input)
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("xxhash3_64_to_64", |b| {
        b.iter_batched(
            || SysRng.try_next_u64().unwrap().to_be_bytes(),
            |data| xxhash3_64(&data),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
