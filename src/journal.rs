use std::{
    mem,
    path::{Path, PathBuf},
};

use crc32fast as crc32;
use libm::ceil;
use memmap2::Mmap;
use tokio::{
    fs::{OpenOptions, remove_file},
    io,
};
use tracing::{info, warn};
use xxhash_rust::xxh3::xxh3_64;
use zerocopy::{FromBytes, Immutable, IntoBytes, LittleEndian, U32, U64};

use crate::{
    BatchingParameter, HashSetMemMap, NB_KEY_IN_EACH_GROUP,
    direct_file::{DirectFile, SECTOR_SIZE},
};

#[derive(Immutable, IntoBytes, FromBytes)]
#[repr(C)]
pub struct SlotId {
    id: U64<LittleEndian>,
}
impl SlotId {
    pub fn from(group_id: u64, group_slot_idx: u64) -> Self {
        SlotId {
            id: (group_id * NB_KEY_IN_EACH_GROUP as u64 + group_slot_idx).into(),
        }
    }

    pub fn get_group_id(&self) -> usize {
        (self.id.get() >> 4) as usize
    }

    pub fn get_group_slot_idx(&self) -> usize {
        (self.id.get() & 0b1111) as usize
    }
}

#[derive(Immutable, IntoBytes, FromBytes)]
#[repr(C)]
pub struct JournalLog {
    pub slot_id: SlotId,
    pub key: U64<LittleEndian>,
}
const JOURNAL_LOG_SIZE: usize = size_of::<JournalLog>();

#[derive(Immutable, IntoBytes, FromBytes, Debug)]
#[repr(C)]
pub struct JournalHeader {
    pub nb_log: U32<LittleEndian>,
}
const JOURNAL_HEADER_SIZE: usize = size_of::<JournalHeader>();

type IntergrityCheckType = U32<LittleEndian>;
const INTEGRITY_CHECK_SIZE: usize = size_of::<IntergrityCheckType>();

pub struct JournalManager {
    journal_file: DirectFile,
    journal_file_path: PathBuf,
    id: u32,
    nb_log: u32,
    destroy_on_drop: bool,
}

impl JournalManager {
    pub async fn new(
        directory_path: &Path,
        journal_id: u32,
        batching_param: BatchingParameter,
    ) -> io::Result<Self> {
        let journal_file_path =
            Path::new(directory_path).join(format!("journal-{:}.bin", journal_id));
        let mut journal_file = DirectFile::new(
            &journal_file_path,
            nb_allocated_sector_for_batching_param(batching_param),
        )
        .await?;
        journal_file.skip(JOURNAL_HEADER_SIZE);

        Ok(Self {
            journal_file,
            journal_file_path,
            id: journal_id,
            nb_log: 0,
            destroy_on_drop: false,
        })
    }

    pub async fn open(
        directory_path: &Path,
        journal_id: u32,
        batching_param: BatchingParameter,
        hashset_mmap: &mut HashSetMemMap,
    ) -> io::Result<Option<Self>> {
        let journal_file_path =
            Path::new(directory_path).join(format!("journal-{:}.bin", journal_id));
        let check_result = check_journal_file(&journal_file_path, hashset_mmap).await?;

        if check_result.file_corrupted {
            warn!(
                "Journal file corrupted from index {}",
                check_result.end_file_idx
            );
        }

        if !check_result.change_detected {
            info!("Journal file does not bring any change");
            let _ = remove_file(journal_file_path).await;
            return Ok(None);
        }

        info!("Journal file does bring change");
        if check_result.file_corrupted {
            warn!(
                "Truncate journal file after the index {}",
                check_result.end_file_idx
            );
        }
        let mut journal_file = DirectFile::from_file(
            &journal_file_path,
            nb_allocated_sector_for_batching_param(batching_param),
            check_result.end_file_idx,
        )
        .await?;
        journal_file.skip(JOURNAL_HEADER_SIZE); //reserve header space

        Ok(Some(Self {
            journal_file,
            journal_file_path,
            id: journal_id,
            nb_log: 0,
            destroy_on_drop: false,
        }))
    }

    pub fn add_log(&mut self, log: JournalLog) {
        self.nb_log += 1;
        self.journal_file.write_slice(log.as_bytes());
    }

    pub async fn finalize(&mut self) -> io::Result<()> {
        let header = JournalHeader {
            nb_log: self.nb_log.into(),
        };
        self.journal_file.get_buffer()[0..JOURNAL_HEADER_SIZE].copy_from_slice(header.as_bytes()); //set header

        let crc32_integrity_check: IntergrityCheckType =
            crc32::hash(self.journal_file.get_buffer()).into();
        self.journal_file
            .write_slice(crc32_integrity_check.as_bytes()); //add integrity check at the end

        let write_res = self.journal_file.write_on_disk().await;

        //reset data
        self.nb_log = 0;
        self.journal_file.skip(JOURNAL_HEADER_SIZE); //reserve header space

        write_res
    }

    pub fn journal_size(&self) -> u64 {
        self.journal_file.file_size()
    }

    pub fn active_delete_on_drop(&mut self) {
        self.destroy_on_drop = true;
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }
}

impl Drop for JournalManager {
    fn drop(&mut self) {
        if self.destroy_on_drop {
            let journal_file_path = mem::take(&mut self.journal_file_path);
            let journal_id = self.id;
            tokio::spawn(async move {
                let res = remove_file(journal_file_path).await;
                match res {
                    Ok(_) => info!("journal file (id:{}) deleted", journal_id),
                    Err(error) => warn!(
                        "error while try to delete journal file (id:{}): {}",
                        journal_id, error
                    ),
                }
            });
        }
    }
}

struct CheckResult {
    change_detected: bool,
    file_corrupted: bool,
    end_file_idx: usize,
}

async fn check_journal_file(
    journal_file_path: &Path,
    hashset_mmap: &mut HashSetMemMap,
) -> io::Result<CheckResult> {
    let journal_file = OpenOptions::new()
        .read(true)
        .open(&journal_file_path)
        .await?;
    //TODO: add SEQUANTIAL FLAG for memmap optimization

    let mut change_detected = false;
    let mut file_corrupted = false;

    let journal_file_mmap = unsafe { Mmap::map(&journal_file)? };
    let journal_file_length = journal_file_mmap.len();
    let mut read_idx = 0;
    while read_idx < journal_file_length {
        if read_idx + JOURNAL_HEADER_SIZE > journal_file_length {
            break;
        }

        let header = JournalHeader::read_from_bytes(
            &journal_file_mmap[read_idx..(read_idx + JOURNAL_HEADER_SIZE)],
        )
        .unwrap(); //safe unwrap

        let nb_log = header.nb_log.get() as usize;
        if nb_log == 0 {
            break;
        }

        if read_idx + JOURNAL_HEADER_SIZE + nb_log * JOURNAL_LOG_SIZE + INTEGRITY_CHECK_SIZE
            > journal_file_length
        {
            warn!("Journal file truncated");
            file_corrupted = true;
            break;
        }

        let current_integrity_check = crc32::hash(
            &journal_file_mmap
                [read_idx..(read_idx + JOURNAL_HEADER_SIZE + nb_log * JOURNAL_LOG_SIZE)],
        );

        let file_integrity_check = IntergrityCheckType::read_from_bytes(
            &journal_file_mmap[(read_idx + JOURNAL_HEADER_SIZE + nb_log * JOURNAL_LOG_SIZE)
                ..(read_idx
                    + JOURNAL_HEADER_SIZE
                    + nb_log * JOURNAL_LOG_SIZE
                    + INTEGRITY_CHECK_SIZE)],
        )
        .unwrap() //safe unwrap
        .get();

        if current_integrity_check != file_integrity_check {
            warn!("Journal file integrity violated");
            file_corrupted = true;
            break;
        }

        read_idx += JOURNAL_HEADER_SIZE;

        for _ in 0..nb_log {
            let log = JournalLog::read_from_bytes(
                &journal_file_mmap[read_idx..(read_idx + JOURNAL_LOG_SIZE)],
            )
            .unwrap(); //safe unwrap

            let slot_id = log.slot_id;
            let key = log.key.get();
            let ctrl = xxh3_64(&key.to_le_bytes()) as u8 | 0b10_00_00_00;

            let mut group = hashset_mmap.group(slot_id.get_group_id());
            if group.get_ctrl(slot_id.get_group_slot_idx()) != ctrl
                || group.get_key(slot_id.get_group_slot_idx()) != key
            {
                println!("{}", key);
                change_detected = true;
                group.set(ctrl, key, slot_id.get_group_slot_idx());
            }

            read_idx += JOURNAL_LOG_SIZE;
        }

        read_idx += INTEGRITY_CHECK_SIZE;
    }

    Ok(CheckResult {
        change_detected,
        file_corrupted,
        end_file_idx: read_idx,
    })
}

fn nb_allocated_sector_for_batching_param(batching_param: BatchingParameter) -> usize {
    ceil(
        (JOURNAL_HEADER_SIZE
            + batching_param.pre_allocated_size * JOURNAL_LOG_SIZE
            + INTEGRITY_CHECK_SIZE) as f64
            / SECTOR_SIZE as f64,
    ) as usize
        + 1
}
