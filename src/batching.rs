use std::{
    arch::x86_64::{__m128i, _mm_loadu_si128},
    collections::HashMap,
    ptr,
};

use xxhash_rust::xxh3::xxh3_64;

use crate::{
    DELETE_FLAG, EMPTY_FLAG, HashSet, NB_KEY_IN_EACH_GROUP, journal::JournalLog,
    simd::simd_match_byte,
};

struct GroupPtr {
    ctrl: *mut u8,
    key: *mut u64,
}

struct TemporaryModificationGroup {
    ctrl: [u8; NB_KEY_IN_EACH_GROUP],
    key: [u64; NB_KEY_IN_EACH_GROUP],
}

#[derive(Clone, Copy)]
pub struct BatchingParameter {
    pub pre_allocated_size: usize,
}

pub struct BatchingData {
    temp_modif_hashmap: HashMap<usize, TemporaryModificationGroup>,
    batch_insert_result: Vec<bool>,
}
impl BatchingData {
    pub fn from_param(param: BatchingParameter) -> Self {
        Self {
            temp_modif_hashmap: HashMap::with_capacity(param.pre_allocated_size * 2),
            batch_insert_result: Vec::with_capacity(param.pre_allocated_size),
        }
    }
}

struct BatchingSelectedSlot {
    slot_id: usize,
    group: BatchingGroup,
    key_index_in_group: u8,
}

enum BatchingGroup {
    OnlyRead { group_id: usize },
    ReadWrite { ptr: GroupPtr },
}

impl HashSet {
    pub async fn batch_insert(&mut self, list_key: &[u64]) -> &[bool] {
        self.batching_data.temp_modif_hashmap.clear();
        self.batching_data.batch_insert_result.clear();

        for key in list_key {
            let success = self.batch_insert_one_key(*key).await;
            self.batching_data.batch_insert_result.push(success);
        }

        self.journal_manager.finalize().await.unwrap();

        //update file mmap
        for (&group_id, modif) in &self.batching_data.temp_modif_hashmap {
            unsafe {
                ptr::copy_nonoverlapping(
                    modif.ctrl.as_ptr(),
                    self.mmap.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP),
                    NB_KEY_IN_EACH_GROUP,
                );
                ptr::copy_nonoverlapping(
                    modif.key.as_ptr(),
                    self.mmap.key.add(group_id * NB_KEY_IN_EACH_GROUP),
                    NB_KEY_IN_EACH_GROUP,
                );
            }
        }

        &self.batching_data.batch_insert_result
    }

    async fn batch_insert_one_key(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_le_bytes()) as usize;
        let h2: u8 = key_hash as u8 & 0b01_11_11_11;

        let mut selected_slot_opt: Option<BatchingSelectedSlot> = None;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;
        loop {
            let (is_on_mmap, group_ptr) = self
                .batching_data
                .temp_modif_hashmap
                .get_mut(&group_id)
                .map_or(
                    (true, unsafe {
                        GroupPtr {
                            ctrl: self.mmap.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP),
                            key: self.mmap.key.add(group_id * NB_KEY_IN_EACH_GROUP),
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
                    .trailing_zeros() as u8;
                    if is_on_mmap {
                        selected_slot_opt = Some(BatchingSelectedSlot {
                            slot_id: group_id * 16 + key_index_in_group as usize,
                            group: BatchingGroup::OnlyRead { group_id },
                            key_index_in_group,
                        });
                    } else {
                        selected_slot_opt = Some(BatchingSelectedSlot {
                            slot_id: group_id * 16 + key_index_in_group as usize,
                            group: BatchingGroup::ReadWrite { ptr: group_ptr },
                            key_index_in_group,
                        });
                    }
                }
                break;
            }

            if selected_slot_opt.is_none() {
                let delete_mask = unsafe { simd_match_byte(ctrl_group_simd, DELETE_FLAG) };
                if delete_mask != 0 {
                    let key_index_in_group = delete_mask.trailing_zeros() as u8;
                    if is_on_mmap {
                        selected_slot_opt = Some(BatchingSelectedSlot {
                            slot_id: group_id * 16 + key_index_in_group as usize,
                            group: BatchingGroup::OnlyRead { group_id },
                            key_index_in_group,
                        });
                    } else {
                        selected_slot_opt = Some(BatchingSelectedSlot {
                            slot_id: group_id * 16 + key_index_in_group as usize,
                            group: BatchingGroup::ReadWrite { ptr: group_ptr },
                            key_index_in_group,
                        });
                    }
                }
            }

            nb_probing += 1;
            group_id += nb_probing;
            if group_id >= self.nb_group {
                group_id &= self.nb_group - 1; //nb_group is a pow of 2
            }
        }

        let selected_slot = selected_slot_opt.unwrap(); //safe unwrap
        match selected_slot.group {
            BatchingGroup::OnlyRead { group_id } => {
                let mut ctrl_slice = [0u8; NB_KEY_IN_EACH_GROUP];
                let mut key_slice = [0u64; NB_KEY_IN_EACH_GROUP];
                unsafe {
                    ptr::copy_nonoverlapping(
                        self.mmap.ctrl.add(group_id * NB_KEY_IN_EACH_GROUP),
                        ctrl_slice.as_mut_ptr(),
                        NB_KEY_IN_EACH_GROUP,
                    );
                    ptr::copy_nonoverlapping(
                        self.mmap.key.add(group_id * NB_KEY_IN_EACH_GROUP),
                        key_slice.as_mut_ptr(),
                        NB_KEY_IN_EACH_GROUP,
                    );
                }
                ctrl_slice[selected_slot.key_index_in_group as usize] = h2;
                key_slice[selected_slot.key_index_in_group as usize] = key;
                self.batching_data.temp_modif_hashmap.insert(
                    group_id,
                    TemporaryModificationGroup {
                        ctrl: ctrl_slice,
                        key: key_slice,
                    },
                );
            }
            BatchingGroup::ReadWrite { ptr: group_ptr } => unsafe {
                *group_ptr
                    .ctrl
                    .add(selected_slot.key_index_in_group as usize) = h2;
                *group_ptr.key.add(selected_slot.key_index_in_group as usize) = key;
            },
        }

        self.journal_manager.add_log(JournalLog {
            slot_id: (selected_slot.slot_id as u64).into(),
            key: key.into(),
        });

        true
    }
}
