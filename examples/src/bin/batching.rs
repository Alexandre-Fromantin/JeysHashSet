use std::{path::Path, time::Instant};

use jeys_hash_set::{BatchingParameter, HashSet};
use tokio::{fs::remove_file, io};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

const DEGREE: u8 = 20;

#[tokio::main]
async fn main() -> io::Result<()> {
    remove_file("data/data.bin").await?;
    remove_file("data/journal.bin").await?;

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let batching_param = BatchingParameter {
        pre_allocated_size: 512,
    };

    let mut hash_set: HashSet = HashSet::new(Path::new("data"), DEGREE, batching_param)
        .await
        .unwrap();

    let chunk_size: u32 = 2u32.pow(9);

    let mut data = Vec::with_capacity(chunk_size as usize);

    let time = Instant::now();
    for i in 0..(2u32.pow(DEGREE as u32 + 3) / chunk_size) {
        data.clear();
        for y in 0..chunk_size {
            data.push(i as u64 * chunk_size as u64 + y as u64);
        }
        hash_set.batch_insert(&data).await;
    }
    let elapsed = time.elapsed();
    println!(
        "time {}: {:?} -> {:?}/insert",
        chunk_size,
        elapsed,
        elapsed / 2u32.pow(DEGREE as u32 + 3)
    );

    drop(hash_set);

    let time = Instant::now();
    let hash_set2: HashSet = HashSet::from_file(Path::new("data"), batching_param)
        .await
        .unwrap();
    println!("time {:?}", time.elapsed());

    Ok(())
}
