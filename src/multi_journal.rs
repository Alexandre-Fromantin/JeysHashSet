use std::{mem::replace, path::Path};

use futures::FutureExt;
use replace_with::replace_with_or_abort;
use sorted_vec::SortedVec;
use tokio::{fs::read_dir, io, task::JoinHandle};

use crate::{
    BatchingParameter, HashSetMemMap,
    flush::FlushManager,
    journal::{JournalLog, JournalManager},
};

pub struct MultiJournalManager {
    current_journal: JournalManager,

    flush_manager: FlushManager,

    batching_param: BatchingParameter,
    journal_creation_state: JournalCreationState,
}

enum JournalCreationState {
    Wait {
        journal_directory: Box<Path>,
    },
    Works {
        create_new_journal_task: JoinHandle<(Box<Path>, io::Result<JournalManager>)>,
    },
}

#[derive(Debug)]
pub enum NewError {
    IOError(io::Error),
    JournalFound,
}

impl From<io::Error> for NewError {
    fn from(error: io::Error) -> Self {
        NewError::IOError(error)
    }
}

#[derive(Debug)]
pub enum OpenError {
    IOError(io::Error),
    JournalFound,
}

impl From<io::Error> for OpenError {
    fn from(error: io::Error) -> Self {
        OpenError::IOError(error)
    }
}

impl MultiJournalManager {
    pub async fn new(
        hash_set_mmap: &HashSetMemMap,
        directory_path: Box<Path>,
        batching_param: BatchingParameter,
    ) -> Result<Self, NewError> {
        if exists_journal_in_directory(&directory_path).await? {
            return Err(NewError::JournalFound);
        }

        Ok(Self {
            current_journal: JournalManager::new(&directory_path, 0, batching_param).await?,
            flush_manager: FlushManager::new(hash_set_mmap),
            batching_param,
            journal_creation_state: JournalCreationState::Wait {
                journal_directory: directory_path,
            },
        })
    }

    pub async fn from_directory(
        hash_set_mmap: &mut HashSetMemMap,
        directory_path: Box<Path>,
        batching_param: BatchingParameter,
    ) -> Result<Self, OpenError> {
        let mut flush_manager = FlushManager::new(hash_set_mmap);
        let mut journal_id_list = SortedVec::new();

        let mut directory = read_dir(&directory_path).await?;
        while let Some(entry) = directory.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                //not a file
                continue;
            }
            let file_name_os = entry.file_name();
            let file_name_opt = file_name_os.to_str();
            if file_name_opt.is_none() {
                //invalid UTF-8 format
                continue;
            }
            let file_name = file_name_opt.unwrap();
            let id_opt = extract_journal_id_from_file_name(file_name);
            let Some(id) = id_opt else {
                //invalid journal name
                continue;
            };
            journal_id_list.push(id);
        }

        let mut current_journal = None;
        for journal_id in journal_id_list {
            let journal =
                JournalManager::open(&directory_path, journal_id, batching_param, hash_set_mmap)
                    .await?;
            let previous_journal_opt = replace(&mut current_journal, journal);
            if let Some(previous_journal) = previous_journal_opt {
                flush_manager.flush(previous_journal);
            }
        }

        Ok(Self {
            current_journal: current_journal
                .unwrap_or(JournalManager::new(&directory_path, 0, batching_param).await?),
            flush_manager,
            batching_param,
            journal_creation_state: JournalCreationState::Wait {
                journal_directory: directory_path,
            },
        })
    }

    pub fn add_log(&mut self, log: JournalLog) {
        self.current_journal.add_log(log);
    }

    pub async fn finalize(&mut self) -> io::Result<()> {
        let finalize_result = self.current_journal.finalize().await;

        if self.current_journal.journal_size() >= 512 * 1024 * 1024 {
            replace_with_or_abort(&mut self.journal_creation_state, |journal_creation_state| {
                match journal_creation_state {
                    JournalCreationState::Wait { journal_directory } => {
                        let batching_param = self.batching_param;
                        let new_id = self.current_journal.get_id() + 1;
                        let create_new_journal_task = tokio::spawn(async move {
                            let journal_res =
                                JournalManager::new(&journal_directory, new_id, batching_param)
                                    .await;
                            (journal_directory, journal_res)
                        });
                        JournalCreationState::Works {
                            create_new_journal_task,
                        }
                    }
                    JournalCreationState::Works {
                        mut create_new_journal_task,
                    } => {
                        if create_new_journal_task.is_finished() {
                            let task_result_opt = (&mut create_new_journal_task).now_or_never();
                            if task_result_opt.is_none() {
                                return JournalCreationState::Works {
                                    create_new_journal_task,
                                };
                            }
                            let task_result = task_result_opt.unwrap().unwrap();

                            let old_journal =
                                replace(&mut self.current_journal, task_result.1.unwrap());
                            self.flush_manager.flush(old_journal);

                            JournalCreationState::Wait {
                                journal_directory: task_result.0,
                            }
                        } else {
                            JournalCreationState::Works {
                                create_new_journal_task,
                            }
                        }
                    }
                }
            });
        }

        finalize_result
    }
}

fn extract_journal_id_from_file_name(s: &str) -> Option<u32> {
    s.strip_prefix("journal-")?
        .strip_suffix(".bin")?
        .parse::<u32>()
        .ok()
}

async fn exists_journal_in_directory(directory_path: &Path) -> io::Result<bool> {
    let mut directory = read_dir(directory_path).await?;
    while let Some(entry) = directory.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() {
            //not a file
            continue;
        }
        let file_name_os = entry.file_name();
        let file_name_opt = file_name_os.to_str();
        if file_name_opt.is_none() {
            //invalid UTF-8 format
            continue;
        }
        let file_name = file_name_opt.unwrap();
        if file_name.starts_with("journal-") {
            return Ok(true);
        }
    }

    Ok(false)
}
