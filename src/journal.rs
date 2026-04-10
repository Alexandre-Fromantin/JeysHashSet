use std::path::Path;

use crc32fast as crc32;
use memmap2::MmapMut;
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncSeekExt, AsyncWriteExt},
};
use tracing::{info, warn};
use xxhash_rust::xxh3::xxh3_64;
use zerocopy::{FromBytes, Immutable, IntoBytes, LittleEndian, U32, U64};

use crate::hashset::HashSetPtr;

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

const MAX_LOG_IN_ONE_TIME: usize = 512;

pub struct JournalManager {
    journal_file: File,
    nb_log: u32,
    write_buffer: Vec<u8>,
}

impl JournalManager {
    pub async fn new(directory_path: &Path, buffer_size: usize) -> io::Result<Self> {
        let journal_file_path = Path::new(directory_path).join("journal.bin");
        let journal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(journal_file_path)
            .await?;

        let mut write_buffer = Vec::with_capacity(
            JOURNAL_HEADER_SIZE + buffer_size * JOURNAL_LOG_SIZE + INTEGRITY_CHECK_SIZE,
        );
        write_buffer.extend_from_slice(&[0u8; JOURNAL_HEADER_SIZE]);

        Ok(Self {
            journal_file,
            nb_log: 0,
            write_buffer,
        })
    }

    pub async fn from_file(
        directory_path: &Path,
        buffer_size: usize,
        data_file_ptr: HashSetPtr,
    ) -> io::Result<Self> {
        let journal_file_path = Path::new(directory_path).join("journal.bin");
        let journal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&journal_file_path)
            .await?;

        let mut no_change = true;

        let journal_file_mmap = unsafe { MmapMut::map_mut(&journal_file)? };
        let journal_file_length = journal_file_mmap.len();
        let mut read_idx = 0;
        while read_idx < journal_file_length {
            if read_idx + JOURNAL_HEADER_SIZE > journal_file_length {
                println!("Journal file truncated");
                break;
            }

            let header = JournalHeader::read_from_bytes(
                &journal_file_mmap[read_idx..(read_idx + JOURNAL_HEADER_SIZE)],
            )
            .unwrap(); //safe unwrap
            let nb_log = header.nb_log.get() as usize;

            if read_idx + JOURNAL_HEADER_SIZE + nb_log * JOURNAL_LOG_SIZE + INTEGRITY_CHECK_SIZE
                > journal_file_length
            {
                warn!("Journal file truncated");
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
                    if !no_change
                        || *data_file_ptr.ctrl.add(slot_id)
                            != (xxh3_64(&key.to_le_bytes()) & 0b01_11_11_11) as u8
                        || *data_file_ptr.key.add(slot_id) != key
                    {
                        no_change = false;
                        *data_file_ptr.ctrl.add(slot_id) =
                            (xxh3_64(&key.to_le_bytes()) & 0b01_11_11_11) as u8;
                        *data_file_ptr.key.add(slot_id) = key;
                    }
                }

                read_idx += JOURNAL_LOG_SIZE;
            }

            read_idx += INTEGRITY_CHECK_SIZE;
        }

        drop(journal_file_mmap);

        if no_change {
            info!("Journal file does not bring any change");
            info!("Empty the journal file and sync it");
            journal_file.set_len(0).await?;
            journal_file.sync_all().await?;
        } else if read_idx < journal_file_length {
            warn!(
                "Journal file corrupted after index {} (journal length: {})",
                read_idx, journal_file_length
            );
            warn!(
                "Truncate journal file after the index {} and sync it",
                read_idx
            );
            journal_file.set_len(read_idx as u64).await?;
            journal_file.sync_all().await?;
        }

        let mut write_buffer = Vec::with_capacity(
            JOURNAL_HEADER_SIZE + buffer_size * JOURNAL_LOG_SIZE + INTEGRITY_CHECK_SIZE,
        );
        write_buffer.extend_from_slice(&[0u8; JOURNAL_HEADER_SIZE]); //pre-fill header

        Ok(Self {
            journal_file,
            nb_log: 0,
            write_buffer,
        })
    }

    pub fn add_log(&mut self, log: JournalLog) {
        self.nb_log += 1;
        self.write_buffer.extend_from_slice(log.as_bytes());
    }

    pub async fn finalize(&mut self) -> io::Result<()> {
        let header = JournalHeader {
            nb_log: self.nb_log.into(),
        };
        self.write_buffer[0..JOURNAL_HEADER_SIZE].copy_from_slice(header.as_bytes()); //set header

        let crc32_integrity_check: IntergrityCheckType = crc32::hash(&self.write_buffer).into();
        self.write_buffer
            .extend_from_slice(crc32_integrity_check.as_bytes()); //add integrity check at the end

        let write_res = self.write_buffer().await;

        //reset data
        self.nb_log = 0;
        self.write_buffer.truncate(JOURNAL_HEADER_SIZE);

        write_res
    }

    async fn write_buffer(&mut self) -> io::Result<()> {
        let seek_pos_before_write = self.journal_file.stream_position().await?;

        if let Err(write_error) = self.journal_file.write_all(&self.write_buffer).await {
            self.journal_file
                .seek(std::io::SeekFrom::Start(seek_pos_before_write))
                .await; //rollback write cursor (if 50% of the buffer in write_all)
            return Err(write_error);
        }

        self.journal_file.sync_all().await?;

        Ok(())
    }
}
