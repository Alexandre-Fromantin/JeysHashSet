use std::{io::SeekFrom, mem::ManuallyDrop, path::Path};

use aligned_vec::{ABox, AVec, ConstAlign};
use libm::{ceil, floor};
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tracing::debug;

pub const SECTOR_SIZE: usize = 4096;
pub const SECTOR_SIZE_U64: u64 = SECTOR_SIZE as u64;

pub struct DirectFile {
    file: File,
    buffer: ManuallyDrop<ABox<[u8], ConstAlign<4096>>>,
    start_sector: u64,
    start_idx: usize,
    write_idx: usize,
}

impl DirectFile {
    pub async fn new(file_path: &Path, nb_sectors_allocated: usize) -> io::Result<Self> {
        let file = open_file_without_os_cache_and_write_sync(file_path, true).await?;

        let mut buffer_vec: AVec<u8, ConstAlign<SECTOR_SIZE>> = AVec::with_capacity(SECTOR_SIZE, 0);
        buffer_vec.resize(SECTOR_SIZE * nb_sectors_allocated, 0);
        let buffer = buffer_vec.into_boxed_slice();

        Ok(DirectFile {
            file,
            buffer: ManuallyDrop::new(buffer),
            start_sector: 0,
            start_idx: 0,
            write_idx: 0,
        })
    }

    pub async fn from_file(
        file_path: &Path,
        nb_sectors_allocated: usize,
        start_idx: usize,
    ) -> io::Result<Self> {
        let mut file = open_file_without_os_cache_and_write_sync(file_path, false).await?;

        let mut buffer_vec: AVec<u8, ConstAlign<SECTOR_SIZE>> = AVec::with_capacity(SECTOR_SIZE, 0);
        buffer_vec.resize(SECTOR_SIZE * nb_sectors_allocated, 0);
        let mut buffer = buffer_vec.into_boxed_slice();

        let start_sector = start_idx as u64 / SECTOR_SIZE_U64;
        let start_idx = start_idx % SECTOR_SIZE;

        file.set_len((start_sector + 1) * SECTOR_SIZE_U64).await?;
        file.seek(SeekFrom::Start(start_sector * SECTOR_SIZE_U64))
            .await?;
        file.read_exact(&mut buffer[..start_idx]).await?;

        Ok(DirectFile {
            file,
            buffer: ManuallyDrop::new(buffer),
            start_sector,
            start_idx,
            write_idx: start_idx,
        })
    }

    pub fn write_slice(&mut self, slice: &[u8]) {
        self.check_enough_space(slice.len());

        self.buffer[self.write_idx..(self.write_idx + slice.len())].copy_from_slice(slice);
        self.write_idx += slice.len();
    }

    pub fn skip(&mut self, n: usize) {
        self.check_enough_space(n);
        self.write_idx += n;
    }

    pub fn get_buffer(&mut self) -> &mut [u8] {
        &mut self.buffer[self.start_idx..self.write_idx]
    }

    fn check_enough_space(&mut self, space_needed: usize) {
        if self.remaining_free_space() < space_needed {
            //not enough space
            debug!("not enough space in write buffer");
            self.add_n_sector(ceil(space_needed as f64 / 4096.0) as usize);
        }
    }

    fn total_capacity(&self) -> usize {
        self.buffer.len()
    }

    fn remaining_free_space(&self) -> usize {
        self.total_capacity() - self.write_idx
    }

    fn add_n_sector(&mut self, nb_new_sector: usize) {
        let mut temp_vec: AVec<u8, ConstAlign<SECTOR_SIZE>> = unsafe {
            AVec::from_raw_parts(
                self.buffer.as_mut_ptr(),
                SECTOR_SIZE,
                self.buffer.len(),
                self.buffer.len(),
            )
        };
        temp_vec.resize(temp_vec.len() + nb_new_sector * SECTOR_SIZE, 0x00);
        self.buffer = ManuallyDrop::new(temp_vec.into_boxed_slice())
    }

    pub async fn write_on_disk(&mut self) -> io::Result<()> {
        let seek_result = self
            .file
            .seek(SeekFrom::Start(self.start_sector * SECTOR_SIZE_U64))
            .await;
        if let Err(seek_error) = seek_result {
            //rollback
            self.buffer[self.start_idx..self.write_idx].fill(0); //reset data
            self.write_idx = self.start_idx; //reset write index
            Err(seek_error)
        } else {
            let nb_sector_used = ceil(self.write_idx as f64 / 4096.0) as usize;

            self.file
                .write_all(&self.buffer[..(nb_sector_used * 4096)])
                .await?;

            self.buffer
                .copy_within(((nb_sector_used - 1) * 4096)..self.write_idx, 0);
            self.buffer[(self.write_idx - ((nb_sector_used - 1) * 4096))..self.write_idx].fill(0);

            self.start_sector += floor(self.write_idx as f64 / 4096.0) as u64;
            self.start_idx = self.write_idx % 4096;
            self.write_idx = self.start_idx;
            Ok(())
        }
    }

    pub fn file_size(&self) -> u64 {
        (self.start_sector + 1) * SECTOR_SIZE_U64
    }
}

impl Drop for DirectFile {
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.buffer) };
    }
}

#[cfg(target_os = "windows")]
async fn open_file_without_os_cache_and_write_sync(
    file_path: &Path,
    new_file: bool,
) -> io::Result<File> {
    use windows::Win32::Storage::FileSystem;

    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(new_file)
        .custom_flags((FileSystem::FILE_FLAG_NO_BUFFERING | FileSystem::FILE_FLAG_WRITE_THROUGH).0)
        .open(file_path)
        .await
}

#[cfg(target_os = "linux")]
async fn open_file_without_os_cache_and_write_sync(
    file_path: &Path,
    new_file: bool,
) -> io::Result<File> {
    use nix::fcntl::OFlag;

    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(new_file)
        .custom_flags((OFlag::O_SYNC | OFlag::O_DIRECT).bits())
        .open(file_path)
        .await
}
