use std::{path::Path, time::Instant};

use tokio::io;

use crate::hashset::HashSet;

mod hashset;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut hash_set: HashSet = HashSet::new(Path::new("data/"), 24).await.unwrap();

    let mut time = Instant::now();
    for i in 0..10_000_000 {
        hash_set.contains(i);
    }
    println!("10M check: {:?}", time.elapsed());

    time = Instant::now();
    for i in 0..10_000 {
        hash_set.insert(i).await;
    }
    println!("10K inserted: {:?}", time.elapsed());

    /*for i in 0..1_000_000 {
        let exists = hash_set.exists(i);
        if !exists {
            println!("error")
        }
    }*/

    Ok(())
}
