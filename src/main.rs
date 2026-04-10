use std::{path::Path, time::Instant};

use tokio::{fs::remove_file, io};

use crate::hashset::HashSet;

mod hashset;
mod journal;

const DEGREE: u8 = 20;

#[tokio::main]
async fn main() -> io::Result<()> {
    /*for x in 7..16 {
        let mut hash_set: HashSet = HashSet::new(Path::new("data"), DEGREE).await.unwrap();

        let chunk_size: u32 = 2u32.pow(x);

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

        let time2 = Instant::now();
        hash_set.fsync_test();
        let elapsed2 = time2.elapsed();
        println!("sync time {:?}", elapsed2);

        remove_file("data/data.bin").await.unwrap();
        remove_file("data/journal.bin").await.unwrap();
    }*/

    let mut hash_set: HashSet = HashSet::new(Path::new("data"), DEGREE).await.unwrap();

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
    let mut hash_set2: HashSet = HashSet::from_file(Path::new("data")).await.unwrap();
    println!("time {:?}", time.elapsed(),);

    Ok(())
}
