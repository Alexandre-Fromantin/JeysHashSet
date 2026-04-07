use memmap2::MmapMut;
use std::arch::x86_64::*;
use std::{io, path::Path};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xxhash_rust::xxh3::xxh3_64;

const DELETE_FLAG: u8 = 0xFE;
const EMPTY_FLAG: u8 = 0xFF;

pub struct HashSet {
    data_file: File,
    journal_file: File,
    data_file_mmap: MmapMut,
    h1_shift: usize,
    nb_group: u64,
    nb_slot: u64,
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

        Ok(Self {
            data_file,
            journal_file,
            data_file_mmap,
            h1_shift: 64 - degree as usize,
            nb_group,
            nb_slot,
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

        let data_file_mmap = unsafe { MmapMut::map_mut(&data_file)? };

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
        })
    }

    pub async fn insert(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_be_bytes());
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut selected_slot_opt: Option<usize> = None;

        let mut group_id = key_hash >> self.h1_shift;
        let mut ctrl_group_ptr =
            unsafe { self.data_file_mmap.as_ptr().add(group_id as usize * 16) };
        let mut nb_probing = 0;
        loop {
            let ctrl_group_simd = unsafe { _mm_loadu_si128(ctrl_group_ptr as *const __m128i) };

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let key_index_in_group = candidate_mask.trailing_zeros() as u64;

                let candidate_key_idx = self.nb_slot as usize
                    + (16 * group_id + key_index_in_group) as usize * size_of::<u64>();

                let candidate_key = u64::from_be_bytes(
                    self.data_file_mmap[candidate_key_idx..(candidate_key_idx + size_of::<u64>())]
                        .try_into()
                        .unwrap(),
                );

                if candidate_key == key {
                    return false;
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

            unsafe {
                nb_probing += 1;
                group_id += nb_probing;
                if group_id < self.nb_group {
                    ctrl_group_ptr = ctrl_group_ptr.add(nb_probing as usize * 16);
                } else {
                    group_id &= self.nb_group - 1; //nb_group is a pow of 2
                    ctrl_group_ptr = self.data_file_mmap.as_ptr().add(group_id as usize * 16);
                }
            }
        }

        if let Some(selected_slot) = selected_slot_opt {
            let mut journal_line_data = [0u8; 8 * 2];
            journal_line_data.as_mut_slice()[0..8].clone_from_slice(&selected_slot.to_be_bytes()); //slot id
            journal_line_data.as_mut_slice()[8..16].clone_from_slice(&key.to_be_bytes()); //value

            self.journal_file
                .write_all(&journal_line_data)
                .await
                .unwrap();
            self.journal_file.sync_all().await.unwrap();

            self.data_file_mmap[selected_slot] = h2;
            let key_idx = (self.nb_slot as usize) + selected_slot * size_of::<u64>();
            self.data_file_mmap[key_idx..(key_idx + size_of::<u64>())]
                .clone_from_slice(&key.to_be_bytes());
        }

        true
    }

    pub fn contains(&self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_be_bytes());
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut group_id = key_hash >> self.h1_shift;
        let mut ctrl_group_ptr =
            unsafe { self.data_file_mmap.as_ptr().add((group_id * 16) as usize) };
        let mut nb_probing = 0;
        loop {
            let ctrl_group_simd = unsafe { _mm_loadu_si128(ctrl_group_ptr as *const __m128i) };

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let key_index_in_group = candidate_mask.trailing_zeros() as u64;

                let candidate_key_idx = (self.nb_slot as usize
                    + (16 * group_id + key_index_in_group) as usize * size_of::<u64>());

                let candidate_key = u64::from_be_bytes(
                    self.data_file_mmap[candidate_key_idx..(candidate_key_idx + size_of::<u64>())]
                        .try_into()
                        .unwrap(),
                );

                if candidate_key == key {
                    return true;
                }

                candidate_mask &= candidate_mask - 1; //remove the 1 most to the right
            }

            let empty_mask = unsafe { simd_match_byte(ctrl_group_simd, EMPTY_FLAG) };
            if empty_mask != 0 {
                return false;
            }

            unsafe {
                nb_probing += 1;
                group_id += nb_probing;
                if group_id < self.nb_group {
                    ctrl_group_ptr = ctrl_group_ptr.add((group_id * 16) as usize);
                } else {
                    group_id &= self.nb_group - 1; //nb_group is a pow of 2
                    ctrl_group_ptr = self.data_file_mmap.as_ptr().add((group_id * 16) as usize);
                }
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
