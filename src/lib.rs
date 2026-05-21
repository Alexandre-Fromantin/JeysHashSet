use memmap2::MmapMut;
use std::arch::x86_64::*;
use std::io::SeekFrom;
use std::sync::Arc;
use std::{io, path::Path};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use xxhash_rust::xxh3::xxh3_64;
use zerocopy::{FromBytes, Immutable, IntoBytes, LittleEndian, U32};

pub mod batching;
mod direct_file;
mod flush;
mod journal;
mod manager;
mod multi_journal;
mod simd;

use journal::JournalLog;

use crate::batching::{BatchingData, BatchingParameter};
use crate::journal::SlotId;
use crate::multi_journal::MultiJournalManager;
use crate::simd::simd_match_byte;

const DELETE_FLAG: u8 = 0b00_00_00_01; //0xFE;
const EMPTY_FLAG: u8 = 0b00_00_00_00; //0xFF;

const NB_KEY_IN_EACH_GROUP: usize = 16;

struct HashSetGroupReadOnly {
    ctrl: *const u8,
    key: *const u64,
}
impl HashSetGroupReadOnly {
    fn get_ctrl(&self, group_slot_idx: usize) -> u8 {
        unsafe { *self.ctrl.add(group_slot_idx) }
    }

    fn get_key(&self, group_slot_idx: usize) -> u64 {
        unsafe { *self.key.add(group_slot_idx) }
    }

    fn load_ctrl_simd(&self) -> __m128i {
        unsafe { _mm_loadu_si128(self.ctrl as *const __m128i) }
    }
}

struct HashSetGroup {
    ctrl: *mut u8,
    key: *mut u64,
}
impl HashSetGroup {
    fn set(&mut self, ctrl: u8, key: u64, group_slot_idx: usize) {
        unsafe {
            *self.ctrl.add(group_slot_idx) = ctrl;
            *self.key.add(group_slot_idx) = key;
        }
    }

    fn set_ctrl(&mut self, ctrl: u8, group_slot_idx: usize) {
        unsafe {
            *self.ctrl.add(group_slot_idx) = ctrl;
        }
    }

    fn get_ctrl(&self, group_slot_idx: usize) -> u8 {
        unsafe { *self.ctrl.add(group_slot_idx) }
    }

    fn get_key(&self, group_slot_idx: usize) -> u64 {
        unsafe { *self.key.add(group_slot_idx) }
    }

    fn load_ctrl_simd(&self) -> __m128i {
        unsafe { _mm_loadu_si128(self.ctrl as *const __m128i) }
    }
}

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
    version: U32<LittleEndian>,
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
        let data_file_path = Path::new(directory_path).join("data.bin");
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(data_file_path)
            .await?;
        //TODO: add RANDOM_ACCESS FLAG for memmap optimization

        let nb_group = 2usize.pow(degree as u32);
        let nb_slot = nb_group * NB_KEY_IN_EACH_GROUP;

        data_file
            .set_len(ALIGNED_CONFIG_SIZE as u64 + (nb_slot * (1 + size_of::<u64>())) as u64)
            .await?;

        let config = HashSetConfig {
            version: 0x00.into(),
            degree,
        };
        data_file.write_all(config.as_bytes()).await.unwrap();
        data_file
            .seek(SeekFrom::Start(ALIGNED_CONFIG_SIZE as u64))
            .await
            .unwrap();

        data_file.sync_all().await.unwrap();

        let mmap = HashSetMemMap::from_file(&data_file)?;
        let journal_manager =
            MultiJournalManager::new(&mmap, directory_path.into(), batching_param)
                .await
                .unwrap();

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

        let mut mmap = HashSetMemMap::from_file(&data_file)?;
        let journal_manager =
            MultiJournalManager::from_directory(&mut mmap, directory_path.into(), batching_param)
                .await
                .unwrap();

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
        let h2: u8 = key_hash as u8 | 0b10_00_00_00;

        let mut selected_slot_opt: Option<(usize, usize)> = None;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let group = self.mmap.group_read_only(group_id);
            let ctrl_simd = group.load_ctrl_simd();

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let group_slot_idx = candidate_mask.trailing_zeros() as usize;

                if group.get_key(group_slot_idx) == key {
                    //the key is already inserted
                    return false;
                }

                candidate_mask &= candidate_mask - 1;
            }

            let empty_mask = unsafe { simd_match_byte(ctrl_simd, EMPTY_FLAG) };
            if empty_mask != 0 {
                if selected_slot_opt.is_none() {
                    let delete_mask = unsafe { simd_match_byte(ctrl_simd, DELETE_FLAG) };
                    let group_slot_idx = if delete_mask != 0 {
                        delete_mask.trailing_zeros()
                    } else {
                        empty_mask.trailing_zeros()
                    } as usize;
                    selected_slot_opt = Some((group_id, group_slot_idx));
                }
                break;
            }

            if selected_slot_opt.is_none() {
                let delete_mask = unsafe { simd_match_byte(ctrl_simd, DELETE_FLAG) };
                if delete_mask != 0 {
                    let group_slot_idx = delete_mask.trailing_zeros() as usize;
                    selected_slot_opt = Some((group_id, group_slot_idx));
                }
            }

            nb_probing += 1;
            group_id += nb_probing;
            if group_id >= self.nb_group {
                group_id &= self.nb_group - 1; //nb_group is a pow of 2
            }
        }

        let (selected_group, selected_group_slot_idx) = selected_slot_opt.unwrap(); //safe unwrap

        self.journal_manager.add_log(JournalLog::Add {
            slot_id: SlotId::from(selected_group as u64, selected_group_slot_idx as u64),
            key,
        });
        self.journal_manager.finalize().await.unwrap();

        let mut group = self.mmap.group(group_id);
        group.set(h2, key, selected_group_slot_idx);

        true
    }

    pub async fn delete(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_le_bytes()) as usize;
        let h2: u8 = key_hash as u8 | 0b10_00_00_00;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let mut group = self.mmap.group(group_id);
            let ctrl_simd = group.load_ctrl_simd();

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let group_slot_idx = candidate_mask.trailing_zeros() as usize;

                if group.get_key(group_slot_idx) == key {
                    //key found

                    let empty_mask = unsafe { simd_match_byte(ctrl_simd, EMPTY_FLAG) };
                    let new_ctrl = if empty_mask != 0 {
                        self.journal_manager.add_log(JournalLog::MakeEmpty {
                            slot_id: SlotId::from(group_id as u64, group_slot_idx as u64),
                        });
                        EMPTY_FLAG
                    } else {
                        self.journal_manager.add_log(JournalLog::Delete {
                            slot_id: SlotId::from(group_id as u64, group_slot_idx as u64),
                        });
                        DELETE_FLAG
                    };
                    self.journal_manager.finalize().await.unwrap();

                    group.set(new_ctrl, 0x00, group_slot_idx);

                    return true;
                }

                candidate_mask &= candidate_mask - 1;
            }

            let empty_mask = unsafe { simd_match_byte(ctrl_simd, EMPTY_FLAG) };
            if empty_mask != 0 {
                break;
            }

            nb_probing += 1;
            group_id += nb_probing;
            if group_id >= self.nb_group {
                group_id &= self.nb_group - 1; //nb_group is a pow of 2
            }
        }

        false
    }

    pub fn contains(&self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_le_bytes()) as usize;
        let h2: u8 = key_hash as u8 | 0b10_00_00_00;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let group = self.mmap.group_read_only(group_id);
            let ctrl_group_simd = group.load_ctrl_simd();

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let group_slot_idx = candidate_mask.trailing_zeros() as usize;

                if group.get_key(group_slot_idx) == key {
                    return true;
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
    data_ptr: *mut u8,
}

unsafe impl Send for HashSetMemMap {}

impl HashSetMemMap {
    pub fn from_file(data_file: &File) -> io::Result<Self> {
        let mut data_file_mmap = unsafe { MmapMut::map_mut(data_file)? };
        let data_ptr = unsafe { data_file_mmap.as_mut_ptr().add(ALIGNED_CONFIG_SIZE) };
        Ok(Self {
            mmap_arc: Arc::new(data_file_mmap),
            data_ptr,
        })
    }

    fn ctrl_group_ptr(&self, group_id: usize) -> *const u8 {
        unsafe {
            self.data_ptr
                .add(group_id * NB_KEY_IN_EACH_GROUP * (1 + size_of::<u64>()))
        }
    }

    fn key_group_ptr(&self, group_id: usize) -> *const u64 {
        unsafe {
            self.data_ptr.add(
                group_id * NB_KEY_IN_EACH_GROUP * (1 + size_of::<u64>()) + NB_KEY_IN_EACH_GROUP,
            ) as *const u64
        }
    }

    pub fn group(&mut self, group_id: usize) -> HashSetGroup {
        HashSetGroup {
            ctrl: self.ctrl_group_ptr(group_id) as *mut u8,
            key: self.key_group_ptr(group_id) as *mut u64,
        }
    }
    pub fn group_read_only(&self, group_id: usize) -> HashSetGroupReadOnly {
        HashSetGroupReadOnly {
            ctrl: self.ctrl_group_ptr(group_id),
            key: self.key_group_ptr(group_id),
        }
    }
}

impl Drop for HashSetMemMap {
    fn drop(&mut self) {
        self.mmap_arc.flush();
    }
}
