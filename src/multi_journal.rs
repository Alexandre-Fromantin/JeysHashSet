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
    transit_state: TransitState,
}

enum TransitState {
    NoTransit {
        journal_directory: Box<Path>,
    },
    CreateNewJournal {
        create_new_journal_task: JoinHandle<(Box<Path>, io::Result<JournalManager>)>,
    },
}

impl MultiJournalManager {
    pub async fn new(
        hash_set_mmap: &HashSetMemMap,
        journal_directory_path: Box<Path>,
        batching_param: BatchingParameter,
    ) -> io::Result<Self> {
        Ok(Self {
            current_journal: JournalManager::new(&journal_directory_path, 0, batching_param)
                .await?,
            flush_manager: FlushManager::new(hash_set_mmap),
            batching_param,
            transit_state: TransitState::NoTransit {
                journal_directory: journal_directory_path,
            },
        })
    }

    pub async fn from_directory(
        hash_set_mmap: &mut HashSetMemMap,
        journal_directory_path: Box<Path>,
        batching_param: BatchingParameter,
    ) -> io::Result<Self> {
        let mut flush_manager = FlushManager::new(hash_set_mmap);
        let mut journal_id_list = SortedVec::new();

        let mut directory = read_dir(&journal_directory_path).await?;
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
            if id_opt.is_none() {
                //invalid journal name
                continue;
            }
            let id = id_opt.unwrap(); //safe unwrap
            journal_id_list.push(id);
        }

        let mut current_journal = None;
        for journal_id in journal_id_list {
            let journal = JournalManager::open(
                &journal_directory_path,
                journal_id,
                batching_param,
                hash_set_mmap,
            )
            .await?;
            let previous_journal_opt = replace(&mut current_journal, journal);
            if let Some(previous_journal) = previous_journal_opt {
                flush_manager.flush(previous_journal);
            }
        }

        Ok(Self {
            current_journal: current_journal
                .unwrap_or(JournalManager::new(&journal_directory_path, 0, batching_param).await?),
            flush_manager,
            batching_param,
            transit_state: TransitState::NoTransit {
                journal_directory: journal_directory_path,
            },
        })
    }

    pub fn add_log(&mut self, log: JournalLog) {
        self.current_journal.add_log(log);
    }

    pub async fn finalize(&mut self) -> io::Result<()> {
        let finalize_result = self.current_journal.finalize().await;

        if self.current_journal.journal_size() >= 512 * 1024 * 1024 {
            replace_with_or_abort(
                &mut self.transit_state,
                |transit_state| match transit_state {
                    TransitState::NoTransit { journal_directory } => {
                        let batching_param = self.batching_param;
                        let new_id = self.current_journal.get_id() + 1;
                        let create_new_journal_task = tokio::spawn(async move {
                            let journal_res =
                                JournalManager::new(&journal_directory, new_id, batching_param)
                                    .await;
                            (journal_directory, journal_res)
                        });
                        TransitState::CreateNewJournal {
                            create_new_journal_task,
                        }
                    }
                    TransitState::CreateNewJournal {
                        mut create_new_journal_task,
                    } => {
                        if create_new_journal_task.is_finished() {
                            let task_result_opt = (&mut create_new_journal_task).now_or_never();
                            if task_result_opt.is_none() {
                                return TransitState::CreateNewJournal {
                                    create_new_journal_task,
                                };
                            }
                            let task_result = task_result_opt.unwrap().unwrap();

                            let old_journal =
                                replace(&mut self.current_journal, task_result.1.unwrap());
                            self.flush_manager.flush(old_journal);

                            TransitState::NoTransit {
                                journal_directory: task_result.0,
                            }
                        } else {
                            TransitState::CreateNewJournal {
                                create_new_journal_task,
                            }
                        }
                    }
                },
            );
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
