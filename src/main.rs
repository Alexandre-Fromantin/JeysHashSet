use std::{fs::OpenOptions, io, path::Path};

use memmap2::MmapMut;

use crate::hashset::HashSet;

mod hashset;

fn main() -> io::Result<()> {
    let hash_set: HashSet<u64> = HashSet::new(Path::new("data/test.bin"), 10).unwrap();

    Ok(())
}
