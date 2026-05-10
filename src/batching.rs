use std::{collections::HashMap, ptr};

use xxhash_rust::xxh3::xxh3_64;

use crate::{
    DELETE_FLAG, EMPTY_FLAG, HashSet, HashSetGroup, HashSetMemMap, NB_KEY_IN_EACH_GROUP,
    journal::{JournalLog, SlotId},
    simd::simd_match_byte,
};

struct TemporaryModificationGroup {
    data: [u8; NB_KEY_IN_EACH_GROUP * (1 + size_of::<u64>())],
}
impl TemporaryModificationGroup {
    fn from_mmap(mmap: &HashSetMemMap, mmap_group_id: usize) -> Self {
        let mut data = [0u8; NB_KEY_IN_EACH_GROUP * (1 + size_of::<u64>())];
        unsafe {
            ptr::copy_nonoverlapping(
                mmap.ctrl_group_ptr(mmap_group_id),
                data.as_mut_ptr(),
                NB_KEY_IN_EACH_GROUP * (1 + size_of::<u64>()),
            );
        }
        Self { data }
    }

    fn apply_on_mmap(&self, mmap: &mut HashSetMemMap, mmap_group_id: usize) {
        unsafe {
            ptr::copy_nonoverlapping(
                self.data.as_ptr(),
                mmap.ctrl_group_ptr(mmap_group_id) as *mut u8,
                NB_KEY_IN_EACH_GROUP * (1 + size_of::<u64>()),
            );
        }
    }

    fn get_ctrl_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }
    fn get_key_ptr(&mut self) -> *mut u64 {
        unsafe { self.data.as_mut_ptr().add(NB_KEY_IN_EACH_GROUP) as *mut u64 }
    }
    fn group(&mut self) -> HashSetGroup {
        HashSetGroup {
            ctrl: self.get_ctrl_ptr(),
            key: self.get_key_ptr(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct BatchingParameter {
    pub pre_allocated_size: usize,
}

pub struct BatchingData {
    temp_modif_hashmap: HashMap<usize, TemporaryModificationGroup>,
    batch_result: Vec<bool>,
}
impl BatchingData {
    pub fn from_param(param: BatchingParameter) -> Self {
        Self {
            temp_modif_hashmap: HashMap::with_capacity(param.pre_allocated_size * 2),
            batch_result: Vec::with_capacity(param.pre_allocated_size),
        }
    }
}

struct BatchingSelectedSlot {
    group: BatchingGroup,
    group_slot_idx: u8,
}

enum BatchingGroup {
    OnlyRead {
        group_id: usize,
    },
    ReadWrite {
        group_id: usize,
        group: HashSetGroup,
    },
}

impl HashSet {
    pub async fn batch_insert(&mut self, list_key: &[u64]) -> &[bool] {
        self.batching_data.temp_modif_hashmap.clear();
        self.batching_data.batch_result.clear();

        for key in list_key {
            let success = self.batch_insert_one_key(*key).await;
            self.batching_data.batch_result.push(success);
        }

        self.journal_manager.finalize().await.unwrap();

        //update file mmap
        for (&group_id, modif) in &self.batching_data.temp_modif_hashmap {
            modif.apply_on_mmap(&mut self.mmap, group_id);
        }

        &self.batching_data.batch_result
    }

    async fn batch_insert_one_key(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_le_bytes()) as usize;
        let h2: u8 = key_hash as u8 | 0b10_00_00_00;

        let mut selected_slot_opt: Option<BatchingSelectedSlot> = None;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;
        loop {
            let (is_on_mmap, group) = self
                .batching_data
                .temp_modif_hashmap
                .get_mut(&group_id)
                .map_or((true, self.mmap.group(group_id)), |temp_group| {
                    (false, temp_group.group())
                });
            let ctrl_group_simd = group.load_ctrl_simd();

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let group_slot_idx = candidate_mask.trailing_zeros() as usize;

                if group.get_key(group_slot_idx) == key {
                    return false;
                }

                candidate_mask &= candidate_mask - 1;
            }
            let empty_mask = unsafe { simd_match_byte(ctrl_group_simd, EMPTY_FLAG) };
            if empty_mask != 0 {
                if selected_slot_opt.is_none() {
                    let delete_mask = unsafe { simd_match_byte(ctrl_group_simd, DELETE_FLAG) };
                    let group_slot_idx = if delete_mask != 0 {
                        delete_mask
                    } else {
                        empty_mask
                    }
                    .trailing_zeros() as u8;
                    if is_on_mmap {
                        selected_slot_opt = Some(BatchingSelectedSlot {
                            group: BatchingGroup::OnlyRead { group_id },
                            group_slot_idx,
                        });
                    } else {
                        selected_slot_opt = Some(BatchingSelectedSlot {
                            group: BatchingGroup::ReadWrite { group, group_id },
                            group_slot_idx,
                        });
                    }
                }
                break;
            }

            if selected_slot_opt.is_none() {
                let delete_mask = unsafe { simd_match_byte(ctrl_group_simd, DELETE_FLAG) };
                if delete_mask != 0 {
                    let group_slot_idx = delete_mask.trailing_zeros() as u8;
                    if is_on_mmap {
                        selected_slot_opt = Some(BatchingSelectedSlot {
                            group: BatchingGroup::OnlyRead { group_id },
                            group_slot_idx,
                        });
                    } else {
                        selected_slot_opt = Some(BatchingSelectedSlot {
                            group: BatchingGroup::ReadWrite { group, group_id },
                            group_slot_idx,
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
        let selected_group_id = match selected_slot.group {
            BatchingGroup::OnlyRead {
                group_id: selected_group_id,
            } => {
                let mut modification =
                    TemporaryModificationGroup::from_mmap(&self.mmap, selected_group_id);

                modification
                    .group()
                    .set(h2, key, selected_slot.group_slot_idx as usize);

                self.batching_data
                    .temp_modif_hashmap
                    .insert(selected_group_id, modification);

                selected_group_id
            }
            BatchingGroup::ReadWrite {
                mut group,
                group_id,
            } => {
                group.set(h2, key, selected_slot.group_slot_idx as usize);
                group_id
            }
        };

        self.journal_manager.add_log(JournalLog::Add {
            slot_id: SlotId::from(
                selected_group_id as u64,
                selected_slot.group_slot_idx as u64,
            ),
            key,
        });

        true
    }

    pub async fn batch_delete(&mut self, list_key: &[u64]) -> &[bool] {
        self.batching_data.temp_modif_hashmap.clear();
        self.batching_data.batch_result.clear();

        for key in list_key {
            let success = self.batch_delete_one_key(*key).await;
            self.batching_data.batch_result.push(success);
        }

        self.journal_manager.finalize().await.unwrap();

        //update file mmap
        for (&group_id, modif) in &self.batching_data.temp_modif_hashmap {
            modif.apply_on_mmap(&mut self.mmap, group_id);
        }

        &self.batching_data.batch_result
    }

    async fn batch_delete_one_key(&mut self, key: u64) -> bool {
        let key_hash = xxh3_64(&key.to_le_bytes()) as usize;
        let h2: u8 = key_hash as u8 | 0b10_00_00_00;

        let mut group_id = key_hash >> self.h1_shift;
        let mut nb_probing = 0;

        loop {
            let (is_on_mmap, mut group) = self
                .batching_data
                .temp_modif_hashmap
                .get_mut(&group_id)
                .map_or((true, self.mmap.group(group_id)), |temp_group| {
                    (false, temp_group.group())
                });
            let ctrl_group_simd = group.load_ctrl_simd();

            let mut candidate_mask = unsafe { simd_match_byte(ctrl_group_simd, h2) };
            while candidate_mask != 0 {
                //Iter on each candidate
                let group_slot_idx = candidate_mask.trailing_zeros() as usize;

                if group.get_key(group_slot_idx) == key {
                    let empty_mask = unsafe { simd_match_byte(ctrl_group_simd, EMPTY_FLAG) };
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

                    if is_on_mmap {
                        let mut modification =
                            TemporaryModificationGroup::from_mmap(&self.mmap, group_id);

                        modification.group().set(new_ctrl, 0x00, group_slot_idx);

                        self.batching_data
                            .temp_modif_hashmap
                            .insert(group_id, modification);
                    } else {
                        group.set(h2, 0x00, group_slot_idx);
                    }

                    return true;
                }

                candidate_mask &= candidate_mask - 1;
            }
            let empty_mask = unsafe { simd_match_byte(ctrl_group_simd, EMPTY_FLAG) };
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
}
