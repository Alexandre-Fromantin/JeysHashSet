use std::{fs::read, path::Path};

use crc32fast as crc32;
use memmap2::{Mmap, MmapMut};
use tokio::{fs::OpenOptions, io};
use tracing::{debug, info, warn};
use xxhash_rust::xxh3::xxh3_64;
use zerocopy::{FromBytes, Immutable, IntoBytes, LittleEndian, U32, U64};

use crate::{HashSetPtr, direct_file::DirectFile};

#[derive(Immutable, IntoBytes, FromBytes)]
#[repr(C)]
pub struct JournalLog {
    pub slot_id: U64<LittleEndian>,
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
    nb_log: u32,
}

impl JournalManager {
    pub async fn new(directory_path: &Path) -> io::Result<Self> {
        let journal_file_path = Path::new(directory_path).join("journal.bin");
        let mut journal_file = DirectFile::new(&journal_file_path, 4).await?;
        journal_file.skip(JOURNAL_HEADER_SIZE);

        Ok(Self {
            journal_file,
            nb_log: 0,
        })
    }

    pub async fn from_file(directory_path: &Path, hash_set_ptr: HashSetPtr) -> io::Result<Self> {
        let journal_file_path = Path::new(directory_path).join("journal.bin");
        let check_result = check_journal_file(&journal_file_path, hash_set_ptr).await?;

        if check_result.file_corrupted {
            warn!(
                "Journal file corrupted from index {}",
                check_result.end_file_idx
            );
        }

        let mut journal_file = if !check_result.change_detected {
            info!("Journal file does bring change");
            if check_result.file_corrupted {
                warn!(
                    "Truncate journal file after the index {}",
                    check_result.end_file_idx
                );
            }
            DirectFile::from_file(&journal_file_path, 4, check_result.end_file_idx).await?
        } else {
            info!("Journal file does not bring any change");
            info!("Empty the journal file");
            DirectFile::from_file(&journal_file_path, 4, 0).await?
        };
        journal_file.skip(JOURNAL_HEADER_SIZE); //reserve header space

        Ok(Self {
            journal_file,
            nb_log: 0,
        })
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
}

struct CheckResult {
    change_detected: bool,
    file_corrupted: bool,
    end_file_idx: usize,
}

async fn check_journal_file(
    journal_file_path: &Path,
    hash_set_ptr: HashSetPtr,
) -> io::Result<CheckResult> {
    let journal_file = OpenOptions::new()
        .read(true)
        .open(&journal_file_path)
        .await?;

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

            let slot_id = log.slot_id.get() as usize;
            let key = log.key.get();

            unsafe {
                if change_detected
                    || *hash_set_ptr.ctrl.add(slot_id)
                        != (xxh3_64(&key.to_le_bytes()) & 0b01_11_11_11) as u8
                    || *hash_set_ptr.key.add(slot_id) != key
                {
                    change_detected = true;
                    *hash_set_ptr.ctrl.add(slot_id) =
                        (xxh3_64(&key.to_le_bytes()) & 0b01_11_11_11) as u8;
                    *hash_set_ptr.key.add(slot_id) = key;
                }
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
