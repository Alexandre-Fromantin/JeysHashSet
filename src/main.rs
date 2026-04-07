use std::{path::Path, time::Instant};

use tokio::{fs::remove_file, io};

use crate::hashset::HashSet;

mod hashset;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut hash_set: HashSet = HashSet::new(Path::new("data/"), 26).await.unwrap();

    let mut data = Vec::with_capacity(512);

    let time = Instant::now();
    for i in 0..100000 {
        data.clear();
        for y in 0..512 {
            data.push(i * 512 + y);
        }
        hash_set.batch_insert(&data).await;
    }
    println!("time: {:?}", time.elapsed());

    remove_file("data/data.bin").await?;
    remove_file("data/journal.bin").await?;

    Ok(())
}
