use std::{
    fs::{File, OpenOptions},
    io,
    marker::PhantomData,
    path::Path,
};

use memmap2::MmapMut;

pub struct HashSet<T> {
    file: File,
    mmap: MmapMut,
    phantom: PhantomData<T>,
}

impl<T> HashSet<T> {
    pub fn new(path: &Path, size: u32) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        file.set_len(2u64.pow(size) * (1 + size_of::<T>() as u64))?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            file,
            mmap,
            phantom: PhantomData,
        })
    }
}
