use either::Either;
use memmap2::MmapMut;
use std::arch::x86_64::*;
use std::collections::HashMap;
use std::ptr;
use std::time::Instant;
use std::{io, path::Path};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xxhash_rust::xxh3::xxh3_64;

const DELETE_FLAG: u8 = 0xFE;
const EMPTY_FLAG: u8 = 0xFF;

struct TempModifGroup {
    ctrl: [u8; 16],
    key: [u64; 16],
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
    nb_group: u64,
    nb_slot: u64,
    ptr: HashSetPtr,
    batching: HashSetBatching,
}

struct HashSetPtr {
    ctrl: *mut u8,
    key: *mut u64,
}

struct HashSetBatching {
    temp_modif_hashmap: HashMap<u64, TempModifGroup>,
    batch_insert_result: Vec<bool>,
    journal_log: Vec<u8>,
}

struct HashSetJournalLine {
    slot_id: u64,
    key: u64,
}

impl HashSet {
    pub async fn new(directory_path: &Path, degree: u32) -> io::Result<Self> {
        let data_file_path = Path::new(directory_path).join("data.bin");
        let data_file = OpenOptions::new()
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

        let nb_group = 2u64.pow(degree);
        let nb_slot = nb_group * 16;

        data_file
            .set_len(nb_slot * (1 + size_of::<u64>() as u64))
            .await?;
        journal_file.set_len(1_000_000).await?;

        let mut data_file_mmap = unsafe { MmapMut::map_mut(&data_file)? };
        data_file_mmap[..(nb_slot as usize)].fill(EMPTY_FLAG);
        let data_file_mmap_ptr = data_file_mmap.as_mut_ptr();

        Ok(Self {
            data_file,
            journal_file,
            data_file_mmap,
            h1_shift: 64 - degree as usize,
            nb_group,
            nb_slot,
            ptr: HashSetPtr {
                ctrl: data_file_mmap_ptr,
                key: unsafe { data_file_mmap_ptr.add(nb_slot as usize) as *mut u64 },
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
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_file_path)
            .await?;

        let journal_file_path = Path::new(directory_path).join("journal.bin");
        let mut journal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(journal_file_path)
            .await?;

        let data_file_len = data_file.metadata().await?.len();
        let nb_slot = data_file_len / (1 + size_of::<u64>() as u64);
        let nb_group = data_file_len / 16;
        let degree = nb_group.trailing_zeros();

        let mut data_file_mmap = unsafe { MmapMut::map_mut(&data_file)? };
        let data_file_mmap_ptr = data_file_mmap.as_mut_ptr();

        let mut journal_read_buf = vec![0u8; 16 * 1024].into_boxed_slice();
        loop {
            let read_size = journal_file.read(&mut journal_read_buf).await?;

            for _ in 0..read_size / 16 {}

            if read_size != 16 * 1024 {
                break;
            }
        }

        Ok(Self {
            data_file,
            journal_file,
            data_file_mmap,
            h1_shift: 64 - degree as usize,
            nb_group,
            nb_slot,
            ptr: HashSetPtr {
                ctrl: data_file_mmap_ptr,
                key: unsafe { data_file_mmap_ptr.add(nb_slot as usize) as *mut u64 },
            },
            batching: HashSetBatching {
                temp_modif_hashmap: HashMap::with_capacity(512),
                batch_insert_result: Vec::with_capacity(512),
                journal_log: Vec::with_capacity(512),
            },
        })
    }

    pub async fn insert(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_be_bytes());
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut selected_slot_opt: Option<usize> = None;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let ctrl_group_ptr = unsafe { self.ptr.ctrl.add(group_id as usize * 16) };
            let ctrl_group_simd = unsafe { _mm_loadu_si128(ctrl_group_ptr as *const __m128i) };

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let key_idx_in_group = candidate_mask.trailing_zeros();

                unsafe {
                    if *self
                        .ptr
                        .key
                        .add(16 * group_id as usize + key_idx_in_group as usize)
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
                    selected_slot_opt = Some(group_id as usize * 16 + key_index_in_group as usize);
                }
                break;
            }

            if selected_slot_opt.is_none() {
                let delete_mask = unsafe { simd_match_byte(ctrl_group_simd, DELETE_FLAG) };
                if delete_mask != 0 {
                    let key_index_in_group = delete_mask.trailing_zeros() as usize;
                    selected_slot_opt = Some(group_id as usize * 16 + key_index_in_group as usize);
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
        for (group_id, modif) in &self.batching.temp_modif_hashmap {
            unsafe {
                ptr::copy_nonoverlapping(
                    modif.ctrl.as_ptr(),
                    self.ptr.ctrl.add(*group_id as usize * 16),
                    16,
                );
                ptr::copy_nonoverlapping(
                    modif.key.as_ptr(),
                    self.ptr.key.add(*group_id as usize * 16),
                    16,
                );
            }
        }

        &self.batching.batch_insert_result
    }

    async fn batch_insert_one_key(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_be_bytes());
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut selected_slot_opt: Option<(Either<u64, GroupPtr>, u8)> = None;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;
        loop {
            let (is_on_mmap, group_ptr) =
                self.batching.temp_modif_hashmap.get_mut(&group_id).map_or(
                    (true, unsafe {
                        GroupPtr {
                            ctrl: self.ptr.ctrl.add(group_id as usize * 16),
                            key: self.ptr.key.add(group_id as usize * 16),
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
                let key_idx_in_group = candidate_mask.trailing_zeros();

                unsafe {
                    if *group_ptr.key.add(key_idx_in_group as usize) == key {
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
                        delete_mask.trailing_zeros() as usize
                    } else {
                        empty_mask.trailing_zeros() as usize
                    };
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
                    let key_index_in_group = delete_mask.trailing_zeros() as usize;
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
                let mut ctrl_slice = [0u8; 16];
                let mut key_slice = [0u64; 16];
                unsafe {
                    ptr::copy_nonoverlapping(
                        self.ptr.ctrl.add(group_id as usize * 16),
                        ctrl_slice.as_mut_ptr(),
                        16,
                    );
                    ptr::copy_nonoverlapping(
                        self.ptr.key.add(group_id as usize * 16),
                        key_slice.as_mut_ptr(),
                        16,
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
        let key_hash = xxh3_64(&key.to_be_bytes());
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let ctrl_group_ptr = unsafe { self.ptr.ctrl.add(group_id as usize * 16) };
            let ctrl_group_simd = unsafe { _mm_loadu_si128(ctrl_group_ptr as *const __m128i) };

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let key_idx_in_group = candidate_mask.trailing_zeros() as u64;

                unsafe {
                    if *self
                        .ptr
                        .key
                        .add(16 * group_id as usize + key_idx_in_group as usize)
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
