use memmap2::MmapMut;
use std::arch::x86_64::*;
use std::io::SeekFrom;
use std::sync::Arc;
use std::{io, path::Path};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use windows::Win32::Storage::FileSystem;
use xxhash_rust::xxh3::xxh3_64;
use zerocopy::{FromBytes, Immutable, IntoBytes};

pub mod batching;
mod direct_file;
mod flush;
mod journal;
mod multi_journal;
mod simd;

use journal::JournalLog;

use crate::batching::{BatchingData, BatchingParameter};
use crate::multi_journal::MultiJournalManager;
use crate::simd::simd_match_byte;

const DELETE_FLAG: u8 = 0xFE;
const EMPTY_FLAG: u8 = 0xFF;

const NB_KEY_IN_EACH_GROUP: usize = 16;

pub struct HashSet {
    data_file: File,
    mmap: HashSetMemMap,
    h1_shift: usize,
    nb_group: usize,
    nb_slot: usize,
    journal_manager: MultiJournalManager,
    batching_data: BatchingData,
}

#[derive(IntoBytes, FromBytes, Immutable)]
#[repr(C)]
struct HashSetConfig {
    degree: u8,
}

const CONFIG_SIZE: usize = size_of::<HashSetConfig>();
const ALIGNED_CONFIG_SIZE: usize = CONFIG_SIZE + (64 - CONFIG_SIZE % 64); //cache friendly

impl HashSet {
    pub async fn new(
        directory_path: &Path,
        degree: u8,
        batching_param: BatchingParameter,
    ) -> io::Result<Self> {
        use windows::Win32::Storage::FileSystem;

        let data_file_path = Path::new(directory_path).join("data.bin");
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(FileSystem::FILE_FLAG_RANDOM_ACCESS.0)
            .create_new(true)
            .open(data_file_path)
            .await?;
        //TODO: add RANDOM_ACCESS FLAG for memmap optimization

        let nb_group = 2usize.pow(degree as u32);
        let nb_slot = nb_group * NB_KEY_IN_EACH_GROUP;

        data_file
            .set_len(ALIGNED_CONFIG_SIZE as u64 + (nb_slot * (1 + size_of::<u64>())) as u64)
            .await?;

        let config = HashSetConfig { degree };
        data_file.write_all(config.as_bytes()).await.unwrap();
        data_file
            .seek(SeekFrom::Start(ALIGNED_CONFIG_SIZE as u64))
            .await
            .unwrap();

        let write_buf = vec![EMPTY_FLAG; 8 * 1024].into_boxed_slice();
        for _ in 0..(nb_slot / (8 * 1024)) {
            data_file.write_all(&write_buf).await.unwrap();
        }
        data_file
            .write_all(&write_buf[0..nb_slot % (8 * 1024)])
            .await
            .unwrap();

        data_file.sync_all().await.unwrap();

        let mmap = HashSetMemMap::from_file(&data_file, nb_slot)?;
        let journal_manager =
            MultiJournalManager::new(&mmap, directory_path.into(), batching_param).await?;

        Ok(Self {
            data_file,
            mmap,
            h1_shift: 64 - degree as usize,
            nb_group,
            nb_slot,
            journal_manager,
            batching_data: BatchingData::from_param(batching_param),
        })
    }

    pub async fn from_file(
        directory_path: &Path,
        batching_param: BatchingParameter,
    ) -> io::Result<Self> {
        let data_file_path = Path::new(directory_path).join("data.bin");
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_file_path)
            .await?;

        let mut config_bytes = [0u8; CONFIG_SIZE];
        data_file.read_exact(&mut config_bytes).await.unwrap();
        let config = HashSetConfig::read_from_bytes(&config_bytes).unwrap();

        let nb_group = 2usize.pow(config.degree as u32);
        let nb_slot = nb_group * NB_KEY_IN_EACH_GROUP;

        let mmap = HashSetMemMap::from_file(&data_file, nb_slot)?;
        let journal_manager =
            MultiJournalManager::from_directory(&mmap, directory_path.into(), batching_param)
                .await?;

        Ok(Self {
            data_file,
            mmap,
            h1_shift: 64 - config.degree as usize,
            nb_group,
            nb_slot,
            journal_manager,
            batching_data: BatchingData::from_param(batching_param),
        })
    }

    pub async fn insert(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_le_bytes()) as usize;
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut selected_slot_opt: Option<usize> = None;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let ctrl_group_ptr = unsafe { self.mmap.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP) };
            let ctrl_group_simd = unsafe { _mm_loadu_si128(ctrl_group_ptr as *const __m128i) };

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let key_idx_in_group = candidate_mask.trailing_zeros() as usize;

                unsafe {
                    if *self
                        .mmap
                        .key
                        .add(NB_KEY_IN_EACH_GROUP * group_id + key_idx_in_group)
                        == key
                    {
                        return false;
                    }
                }

                candidate_mask &= candidate_mask - 1;
            }

            let empty_mask = unsafe { simd_match_byte(ctrl_group_simd, EMPTY_FLAG) };
            if empty_mask != 0 {
                if selected_slot_opt.is_none() {
                    let delete_mask = unsafe { simd_match_byte(ctrl_group_simd, DELETE_FLAG) };
                    let key_index_in_group = if delete_mask != 0 {
                        delete_mask.trailing_zeros()
                    } else {
                        empty_mask.trailing_zeros()
                    } as usize;
                    selected_slot_opt = Some(group_id * NB_KEY_IN_EACH_GROUP + key_index_in_group);
                }
                break;
            }

            if selected_slot_opt.is_none() {
                let delete_mask = unsafe { simd_match_byte(ctrl_group_simd, DELETE_FLAG) };
                if delete_mask != 0 {
                    let key_index_in_group = delete_mask.trailing_zeros() as usize;
                    selected_slot_opt = Some(group_id * NB_KEY_IN_EACH_GROUP + key_index_in_group);
                }
            }

            nb_probing += 1;
            group_id += nb_probing;
            if group_id >= self.nb_group {
                group_id &= self.nb_group - 1; //nb_group is a pow of 2
            }
        }

        let selected_slot = selected_slot_opt.unwrap(); //safe unwrap

        self.journal_manager.add_log(JournalLog {
            slot_id: (selected_slot as u64).into(),
            key: key.into(),
        });
        self.journal_manager.finalize().await.unwrap();

        unsafe {
            *self.mmap.ctrl.add(selected_slot) = h2;
            *self.mmap.key.add(selected_slot) = key;
        }

        true
    }

    pub fn contains(&self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_le_bytes()) as usize;
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let ctrl_group_ptr = unsafe { self.mmap.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP) };
            let ctrl_group_simd = unsafe { _mm_loadu_si128(ctrl_group_ptr as *const __m128i) };

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let key_idx_in_group = candidate_mask.trailing_zeros() as usize;

                unsafe {
                    if *self
                        .mmap
                        .key
                        .add(NB_KEY_IN_EACH_GROUP * group_id + key_idx_in_group)
                        == key
                    {
                        return false;
                    }
                }

                candidate_mask &= candidate_mask - 1; //remove the 1 most to the right
            }

            let empty_mask = unsafe { simd_match_byte(ctrl_group_simd, EMPTY_FLAG) };
            if empty_mask != 0 {
                return false;
            }

            nb_probing += 1;
            group_id += nb_probing;
            if group_id >= self.nb_group {
                group_id &= self.nb_group - 1; //nb_group is a pow of 2
            }
        }
    }
}

struct HashSetMemMap {
    mmap_arc: Arc<MmapMut>,
    pub ctrl: *mut u8,
    pub key: *mut u64,
}

impl HashSetMemMap {
    pub fn from_file(data_file: &File, nb_slot: usize) -> io::Result<Self> {
        let mut data_file_mmap = unsafe { MmapMut::map_mut(data_file)? };
        let ctrl_ptr = unsafe { data_file_mmap.as_mut_ptr().add(ALIGNED_CONFIG_SIZE) };
        let key_ptr = unsafe { ctrl_ptr.add(nb_slot) as *mut u64 };
        Ok(Self {
            mmap_arc: Arc::new(data_file_mmap),
            ctrl: ctrl_ptr,
            key: key_ptr,
        })
    }
}

impl Drop for HashSetMemMap {
    fn drop(&mut self) {
        self.mmap_arc.flush().unwrap();
    }
}
