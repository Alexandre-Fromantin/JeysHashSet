use std::{
    path::Path,
    time::{Duration, Instant},
};

use jeys_hash_set::{HashSet, batching::BatchingParameter};
use tokio::{fs, io};
use tracing::{Level, debug, info};
use tracing_subscriber::FmtSubscriber;

const DEGREE: u8 = 20;
const NB_INSERT: u32 = 2u32.pow(DEGREE as u32 + 3);

#[tokio::main]
async fn main() -> io::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    for batching_size_i in 9..17 {
        let batching_size: u32 = 2u32.pow(batching_size_i);

        let batching_param = BatchingParameter {
            pre_allocated_size: batching_size as usize,
        };

        let mut hash_set: HashSet = HashSet::new(Path::new("data"), DEGREE, batching_param)
            .await
            .unwrap();

        let mut data = Vec::with_capacity(batching_size as usize);

        let mut time = Instant::now();
        for i in 0..(NB_INSERT / batching_size) {
            data.clear();
            for y in 0..batching_size {
                data.push(i as u64 * batching_size as u64 + y as u64);
            }
            hash_set.batch_insert(&data).await;
        }
        let elapsed = time.elapsed();
        info!(
            "for batching size: {} | {} insertion time: {:?} | {:?}/insert",
            batching_size,
            NB_INSERT,
            elapsed,
            elapsed / NB_INSERT
        );
        println!();

        for i in 0..NB_INSERT as u64 {
            let exist = hash_set.contains(i);
            if !exist {
                info!("error")
            }
        }

        time = Instant::now();
        for i in 0..(NB_INSERT / batching_size) {
            data.clear();
            for y in 0..batching_size {
                data.push(i as u64 * batching_size as u64 + y as u64);
            }
            hash_set.batch_delete(&data).await;
        }
        let elapsed = time.elapsed();
        info!(
            "for batching size: {} | {} delete time: {:?} | {:?}/delete",
            batching_size,
            NB_INSERT,
            elapsed,
            elapsed / NB_INSERT
        );
        println!();

        for i in 0..NB_INSERT as u64 {
            let exist = hash_set.contains(i);
            if exist {
                info!("error")
            }
        }

        drop(hash_set);

        time = Instant::now();
        let hash_set2: HashSet = HashSet::from_file(Path::new("data"), batching_param)
            .await
            .unwrap();
        info!("recovery time {:?}", time.elapsed());
        println!();
    }

    Ok(())
}
