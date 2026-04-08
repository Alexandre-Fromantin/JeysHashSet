use either::Either;
use memmap2::MmapMut;
use std::arch::x86_64::*;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::ptr;
use std::time::Instant;
use std::{io, path::Path};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use xxhash_rust::xxh3::xxh3_64;
use zerocopy::{FromBytes, Immutable, IntoBytes};

const DELETE_FLAG: u8 = 0xFE;
const EMPTY_FLAG: u8 = 0xFF;

const NB_KEY_IN_EACH_GROUP: usize = 16;

#[derive(IntoBytes, FromBytes, Immutable)]
#[repr(C)]
struct HashSetConfig {
    degree: u8,
}

const CONFIG_SIZE: usize = size_of::<HashSetConfig>();
const ALIGNED_CONFIG_SIZE: usize = CONFIG_SIZE + (64 - CONFIG_SIZE % 64); //cache friendly

struct TempModifGroup {
    ctrl: [u8; NB_KEY_IN_EACH_GROUP],
    key: [u64; NB_KEY_IN_EACH_GROUP],
}

struct GroupPtr {
    ctrl: *mut u8,
    key: *mut u64,
}

pub struct HashSet {
    data_file: File,
    journal_file: File,
    data_file_mmap: MmapMut,
    h1_shift: usize,
    nb_group: usize,
    nb_slot: usize,
    ptr: HashSetPtr,
    batching: HashSetBatching,
}

struct HashSetPtr {
    ctrl: *mut u8,
    key: *mut u64,
}

struct HashSetBatching {
    temp_modif_hashmap: HashMap<usize, TempModifGroup>,
    batch_insert_result: Vec<bool>,
    journal_log: Vec<u8>,
}

struct HashSetJournalLine {
    slot_id: u64,
    key: u64,
}

impl HashSet {
    pub async fn new(directory_path: &Path, degree: u8) -> io::Result<Self> {
        let data_file_path = Path::new(directory_path).join("data.bin");
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(data_file_path)
            .await?;

        let journal_file_path = Path::new(directory_path).join("journal.bin");
        let journal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(journal_file_path)
            .await?;

        let nb_group = 2usize.pow(degree as u32);
        let nb_slot = nb_group * NB_KEY_IN_EACH_GROUP;

        data_file
            .set_len((nb_slot * (1 + size_of::<u64>())) as u64)
            .await?;
        journal_file.set_len(1_000_000).await?;

        let config = HashSetConfig { degree };
        data_file.write_all(config.as_bytes()).await.unwrap();
        data_file
            .seek(SeekFrom::Start(ALIGNED_CONFIG_SIZE as u64))
            .await
            .unwrap(); //cache friendly for mmap

        let write_buf = vec![EMPTY_FLAG; 8 * 1024].into_boxed_slice();
        for i in 0..(nb_slot / (8 * 1024)) {
            data_file.write_all(&write_buf).await.unwrap();
        }
        data_file
            .write_all(&write_buf[0..nb_slot % (8 * 1024)])
            .await
            .unwrap();

        data_file.sync_all().await.unwrap();

        let mut data_file_mmap = unsafe { MmapMut::map_mut(&data_file)? };
        let ctrl_ptr = unsafe { data_file_mmap.as_mut_ptr().add(ALIGNED_CONFIG_SIZE) };

        Ok(Self {
            data_file,
            journal_file,
            data_file_mmap,
            h1_shift: 64 - degree as usize,
            nb_group,
            nb_slot,
            ptr: HashSetPtr {
                ctrl: ctrl_ptr,
                key: unsafe { ctrl_ptr.add(nb_slot) as *mut u64 },
            },
            batching: HashSetBatching {
                temp_modif_hashmap: HashMap::new(),
                batch_insert_result: Vec::with_capacity(512),
                journal_log: Vec::with_capacity(512),
            },
        })
    }

    pub async fn from_file(directory_path: &Path) -> io::Result<Self> {
        let data_file_path = Path::new(directory_path).join("data.bin");
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_file_path)
            .await?;

        let journal_file_path = Path::new(directory_path).join("journal.bin");
        let journal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(journal_file_path)
            .await?;

        let mut config_bytes = [0u8; CONFIG_SIZE];
        data_file.read_exact(&mut config_bytes).await.unwrap();
        let config = HashSetConfig::read_from_bytes(&config_bytes).unwrap();

        let nb_group = 2usize.pow(config.degree as u32);
        let nb_slot = nb_group * NB_KEY_IN_EACH_GROUP;

        let mut data_file_mmap = unsafe { MmapMut::map_mut(&data_file)? };
        let ctrl_ptr = unsafe { data_file_mmap.as_mut_ptr().add(ALIGNED_CONFIG_SIZE) };

        /*let mut journal_read_buf = vec![0u8; 16 * 1024].into_boxed_slice();
        loop {
            let read_size = journal_file.read(&mut journal_read_buf).await?;

            for _ in 0..read_size / 16 {}

            if read_size != 16 * 1024 {
                break;
            }
        }*/

        Ok(Self {
            data_file,
            journal_file,
            data_file_mmap,
            h1_shift: 64 - config.degree as usize,
            nb_group,
            nb_slot,
            ptr: HashSetPtr {
                ctrl: ctrl_ptr,
                key: unsafe { ctrl_ptr.add(nb_slot) as *mut u64 },
            },
            batching: HashSetBatching {
                temp_modif_hashmap: HashMap::with_capacity(512),
                batch_insert_result: Vec::with_capacity(512),
                journal_log: Vec::with_capacity(512),
            },
        })
    }

    pub async fn insert(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_be_bytes()) as usize;
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut selected_slot_opt: Option<usize> = None;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let ctrl_group_ptr = unsafe { self.ptr.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP) };
            let ctrl_group_simd = unsafe { _mm_loadu_si128(ctrl_group_ptr as *const __m128i) };

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let key_idx_in_group = candidate_mask.trailing_zeros() as usize;

                unsafe {
                    if *self
                        .ptr
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

        let mut journal_line_data = [0u8; 8 * 2];
        journal_line_data.as_mut_slice()[0..8].clone_from_slice(&selected_slot.to_be_bytes()); //slot id
        journal_line_data.as_mut_slice()[8..16].clone_from_slice(&key.to_be_bytes()); //value

        self.journal_file
            .write_all(&journal_line_data)
            .await
            .unwrap();
        self.journal_file.sync_all().await.unwrap();

        unsafe {
            *self.ptr.ctrl.add(selected_slot) = h2;
            *self.ptr.key.add(selected_slot) = key;
        }

        true
    }

    pub async fn batch_insert(&mut self, list_key: &[u64]) -> &[bool] {
        self.batching.temp_modif_hashmap.clear();
        self.batching.batch_insert_result.clear();
        self.batching.journal_log.clear();

        for key in list_key {
            let success = self.batch_insert_one_key(*key).await;
            self.batching.batch_insert_result.push(success);
        }

        //add journal logs
        self.journal_file
            .write_all(&self.batching.journal_log)
            .await
            .unwrap();
        self.journal_file.sync_all().await.unwrap();

        //update file mmap
        for (&group_id, modif) in &self.batching.temp_modif_hashmap {
            unsafe {
                ptr::copy_nonoverlapping(
                    modif.ctrl.as_ptr(),
                    self.ptr.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP),
                    NB_KEY_IN_EACH_GROUP,
                );
                ptr::copy_nonoverlapping(
                    modif.key.as_ptr(),
                    self.ptr.key.add(group_id * NB_KEY_IN_EACH_GROUP),
                    NB_KEY_IN_EACH_GROUP,
                );
            }
        }

        &self.batching.batch_insert_result
    }

    async fn batch_insert_one_key(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_be_bytes()) as usize;
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut selected_slot_opt: Option<(Either<usize, GroupPtr>, u8)> = None;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;
        loop {
            let (is_on_mmap, group_ptr) =
                self.batching.temp_modif_hashmap.get_mut(&group_id).map_or(
                    (true, unsafe {
                        GroupPtr {
                            ctrl: self.ptr.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP),
                            key: self.ptr.key.add(group_id * NB_KEY_IN_EACH_GROUP),
                        }
                    }),
                    |temp_group| {
                        (
                            false,
                            GroupPtr {
                                ctrl: temp_group.ctrl.as_mut_ptr(),
                                key: temp_group.key.as_mut_ptr(),
                            },
                        )
                    },
                );
            let ctrl_group_simd = unsafe { _mm_loadu_si128(group_ptr.ctrl as *const __m128i) };

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let key_idx_in_group = candidate_mask.trailing_zeros() as usize;

                unsafe {
                    if *group_ptr.key.add(key_idx_in_group) == key {
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
                        delete_mask
                    } else {
                        empty_mask
                    }
                    .trailing_zeros() as usize;
                    if is_on_mmap {
                        selected_slot_opt =
                            Some((Either::Left(group_id), key_index_in_group as u8));
                    } else {
                        selected_slot_opt =
                            Some((Either::Right(group_ptr), key_index_in_group as u8));
                    }
                }
                break;
            }

            if selected_slot_opt.is_none() {
                let delete_mask = unsafe { simd_match_byte(ctrl_group_simd, DELETE_FLAG) };
                if delete_mask != 0 {
                    let key_index_in_group = delete_mask.trailing_zeros();
                    if is_on_mmap {
                        selected_slot_opt =
                            Some((Either::Left(group_id), key_index_in_group as u8));
                    } else {
                        selected_slot_opt =
                            Some((Either::Right(group_ptr), key_index_in_group as u8));
                    }
                }
            }

            nb_probing += 1;
            group_id += nb_probing;
            if group_id >= self.nb_group {
                group_id &= self.nb_group - 1; //nb_group is a pow of 2
            }
        }

        let (group, slot_idx_in_group) = selected_slot_opt.unwrap(); //safe unwrap
        match group {
            Either::Left(selected_group_id) => {
                let mut ctrl_slice = [0u8; NB_KEY_IN_EACH_GROUP];
                let mut key_slice = [0u64; NB_KEY_IN_EACH_GROUP];
                unsafe {
                    ptr::copy_nonoverlapping(
                        self.ptr.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP),
                        ctrl_slice.as_mut_ptr(),
                        NB_KEY_IN_EACH_GROUP,
                    );
                    ptr::copy_nonoverlapping(
                        self.ptr.key.add(group_id * NB_KEY_IN_EACH_GROUP),
                        key_slice.as_mut_ptr(),
                        NB_KEY_IN_EACH_GROUP,
                    );
                }
                ctrl_slice[slot_idx_in_group as usize] = h2;
                key_slice[slot_idx_in_group as usize] = key;
                self.batching.temp_modif_hashmap.insert(
                    selected_group_id,
                    TempModifGroup {
                        ctrl: ctrl_slice,
                        key: key_slice,
                    },
                );
            }
            Either::Right(group_ptr) => unsafe {
                *group_ptr.ctrl.add(slot_idx_in_group as usize) = h2;
                *group_ptr.key.add(slot_idx_in_group as usize) = key;
            },
        }

        self.batching.journal_log.extend_from_slice(&[0u8; 16]);

        true
    }

    pub fn contains(&self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_be_bytes()) as usize;
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let ctrl_group_ptr = unsafe { self.ptr.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP) };
            let ctrl_group_simd = unsafe { _mm_loadu_si128(ctrl_group_ptr as *const __m128i) };

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let key_idx_in_group = candidate_mask.trailing_zeros() as usize;

                unsafe {
                    if *self
                        .ptr
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

unsafe fn simd_match_byte(simd_data: __m128i, byte: u8) -> u16 {
    unsafe {
        let hash_vec = _mm_set1_epi8(byte as i8);

        let cmp = _mm_cmpeq_epi8(simd_data, hash_vec);
        _mm_movemask_epi8(cmp) as u16
    }
}
