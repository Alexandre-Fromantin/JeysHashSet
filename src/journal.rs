use std::{
    io::Cursor,
    mem,
    path::{Path, PathBuf},
};

use crc32fast as crc32;
use deku::ctx::Endian;
use deku::{
    DekuContainerRead, DekuContainerWrite, DekuRead, DekuWrite, DekuWriter, writer::Writer,
};
use libm::ceil;
use memmap2::Mmap;
use tokio::{
    fs::{OpenOptions, remove_file},
    io,
};
use tracing::{info, warn};
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    BatchingParameter, DELETE_FLAG, EMPTY_FLAG, HashSetMemMap, NB_KEY_IN_EACH_GROUP,
    direct_file::{DirectFile, SECTOR_SIZE},
};

#[derive(DekuRead, DekuWrite, Debug)]
#[deku(ctx = "endian: Endian", endian = "endian")]
pub struct SlotId {
    #[deku(bits = "62")]
    id: u64,
}
impl SlotId {
    pub fn from(group_id: u64, group_slot_idx: u64) -> Self {
        SlotId {
            id: (group_id * NB_KEY_IN_EACH_GROUP as u64 + group_slot_idx),
        }
    }

    pub fn from_id(id: u64) -> Self {
        SlotId { id }
    }

    pub fn get_group_id(&self) -> usize {
        (self.id >> 4) as usize
    }

    pub fn get_group_slot_idx(&self) -> usize {
        (self.id & 0b1111) as usize
    }
}

#[derive(DekuRead, DekuWrite, Debug)]
#[deku(id_type = "u8", bits = 2, endian = "little")]
pub enum JournalLog {
    #[deku(id = "0")]
    Delete { slot_id: SlotId },

    #[deku(id = "1")]
    MakeEmpty { slot_id: SlotId },

    #[deku(id = "2")]
    Add { slot_id: SlotId, key: u64 },
}
const MAX_JOURNAL_LOG_SIZE: usize = size_of::<u64>() * 2;

#[derive(DekuRead, DekuWrite, Debug)]
#[deku(endian = "little")]
pub struct JournalHeader {
    pub block_length: u32,
}
const JOURNAL_HEADER_SIZE: usize = size_of::<JournalHeader>();

#[derive(DekuRead, DekuWrite, Debug)]
#[deku(endian = "little")]
pub struct IntegrityFooter {
    crc32: u32,
}
const INTEGRITY_FOOTER_SIZE: usize = size_of::<IntegrityFooter>();

pub struct JournalManager {
    journal_file: DirectFile,
    journal_file_path: PathBuf,
    id: u32,
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

        journal_file.skip(JOURNAL_HEADER_SIZE); //reserve header space

        Ok(Self {
            journal_file,
            journal_file_path,
            id: journal_id,
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
            destroy_on_drop: false,
        }))
    }

    pub fn add_log(&mut self, log: JournalLog) {
        log.to_writer(&mut Writer::new(&mut self.journal_file), ())
            .unwrap();
    }

    pub async fn finalize(&mut self) -> io::Result<()> {
        let writed_slice = self.journal_file.get_buffer();
        let header = JournalHeader {
            block_length: (writed_slice.len() - JOURNAL_HEADER_SIZE) as u32,
        };
        header
            .to_slice(&mut writed_slice[0..JOURNAL_HEADER_SIZE])
            .unwrap(); //set header

        let crc32: u32 = crc32::hash(self.journal_file.get_buffer());
        let integrity_footer = IntegrityFooter { crc32 };
        integrity_footer
            .to_writer(&mut Writer::new(&mut self.journal_file), ())
            .unwrap(); //add integrity check at the end

        let write_res = self.journal_file.write_on_disk().await;

        //reset data
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
    let journal_file_len = journal_file_mmap.len() as u64;
    let mut journal_cursor = Cursor::new(journal_file_mmap);

    while journal_cursor.position() < journal_file_len {
        let start_pos = journal_cursor.position();
        let journal_header_res = JournalHeader::from_reader((&mut journal_cursor, 0));
        if let Err(journal_header_err) = journal_header_res {
            match journal_header_err {
                deku::DekuError::Incomplete(_need_size) => {
                    warn!("Journal file truncated (missing bits for parsing)");
                }
                _ => {
                    warn!(
                        "Journal file corrupted (deku error: {})",
                        journal_header_err
                    );
                }
            }
            file_corrupted = true;
            break;
        }
        let (_, journal_header) = journal_header_res.unwrap();
        let block_length = journal_header.block_length as u64;

        let after_header_pos = journal_cursor.position();
        if after_header_pos + block_length > journal_file_len {
            warn!("Journal file truncated (block not finished)");
            file_corrupted = true;
            break;
        }
        journal_cursor.set_position(after_header_pos + block_length);

        let current_crc32 = crc32::hash(
            &journal_cursor.get_ref()
                [start_pos as usize..((after_header_pos + block_length) as usize)],
        );

        let integrity_footer_res = IntegrityFooter::from_reader((&mut journal_cursor, 0));
        if let Err(integrity_footer_err) = integrity_footer_res {
            match integrity_footer_err {
                deku::DekuError::Incomplete(_need_size) => {
                    warn!("Journal file truncated (missing bits for parsing)");
                }
                _ => {
                    warn!(
                        "Journal file corrupted (deku error: {})",
                        integrity_footer_err
                    );
                }
            }
            file_corrupted = true;
            break;
        }
        let (_, integrity_footer) = integrity_footer_res.unwrap();
        if integrity_footer.crc32 != current_crc32 {
            warn!("Journal file block integrity violated");
            file_corrupted = true;
            break;
        }
        let end_block_pos = journal_cursor.position();

        journal_cursor.set_position(after_header_pos);

        while journal_cursor.position() < after_header_pos + block_length {
            let log_res = JournalLog::from_reader((&mut journal_cursor, 0));
            let Ok((_, log)) = log_res else {
                let log_err = log_res.unwrap_err();
                warn!("Journal file corrupted (deku error: {})", log_err);
                file_corrupted = true;
                break;
            };
            match log {
                JournalLog::Delete { slot_id } => {
                    let ctrl = DELETE_FLAG;

                    let mut group = hashset_mmap.group(slot_id.get_group_id());
                    if group.get_ctrl(slot_id.get_group_slot_idx()) != ctrl
                        || group.get_key(slot_id.get_group_slot_idx()) != 0x00
                    {
                        change_detected = true;
                        group.set(ctrl, 0x00, slot_id.get_group_slot_idx());
                    }
                }
                JournalLog::MakeEmpty { slot_id } => {
                    let ctrl = EMPTY_FLAG;

                    let mut group = hashset_mmap.group(slot_id.get_group_id());
                    if group.get_ctrl(slot_id.get_group_slot_idx()) != ctrl
                        || group.get_key(slot_id.get_group_slot_idx()) != 0x00
                    {
                        change_detected = true;
                        group.set(ctrl, 0x00, slot_id.get_group_slot_idx());
                    }
                }
                JournalLog::Add { slot_id, key } => {
                    let ctrl = xxh3_64(&key.to_le_bytes()) as u8 | 0b10_00_00_00;

                    let mut group = hashset_mmap.group(slot_id.get_group_id());
                    if group.get_ctrl(slot_id.get_group_slot_idx()) != ctrl
                        || group.get_key(slot_id.get_group_slot_idx()) != key
                    {
                        change_detected = true;
                        group.set(ctrl, key, slot_id.get_group_slot_idx());
                    }
                }
            }
        }
        if file_corrupted {
            break;
        }

        journal_cursor.set_position(end_block_pos);
    }

    Ok(CheckResult {
        change_detected,
        file_corrupted,
        end_file_idx: journal_cursor.position() as usize,
    })
}

fn nb_allocated_sector_for_batching_param(batching_param: BatchingParameter) -> usize {
    ceil(
        (JOURNAL_HEADER_SIZE
            + batching_param.pre_allocated_size * MAX_JOURNAL_LOG_SIZE
            + INTEGRITY_FOOTER_SIZE) as f64
            / SECTOR_SIZE as f64,
    ) as usize
        + 1
}
