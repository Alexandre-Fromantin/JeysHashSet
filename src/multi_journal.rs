use std::{mem::replace, path::Path, sync::Arc};

use futures::FutureExt;
use memmap2::MmapMut;
use replace_with::replace_with_or_abort;
use tokio::{fs::read_dir, io, sync::oneshot, task::JoinHandle};
use tracing::debug;

use crate::{
    BatchingParameter, FlushManager,
    journal::{JournalLog, JournalManager},
};

pub struct MultiJournalManager {
    current_journal: JournalManager,
    old_journal_opt: Option<(JournalManager, oneshot::Receiver<Result<(), ()>>)>,

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
        memmap_arc: &Arc<MmapMut>,
        journal_directory_path: Box<Path>,
        batching_param: BatchingParameter,
    ) -> io::Result<Self> {
        Ok(Self {
            current_journal: JournalManager::new(&journal_directory_path, 0, batching_param)
                .await?,
            old_journal_opt: None,
            flush_manager: FlushManager::new(memmap_arc),
            batching_param,
            transit_state: TransitState::NoTransit {
                journal_directory: journal_directory_path,
            },
        })
    }

    pub async fn from_directory(
        memmap_arc: &Arc<MmapMut>,
        journal_directory_path: Box<Path>,
        batching_param: BatchingParameter,
    ) -> io::Result<Self> {
        let mut directory = read_dir(&journal_directory_path).await?;
        while let Some(entry) = directory.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                continue;
            }
            let file_name_os = entry.file_name();
            if file_name_os.len() < 8 + 4 + 1 {
                continue;
            }
            let file_name_opt = file_name_os.to_str();
            if file_name_opt.is_none() {
                continue;
            }
            let file_name = file_name_opt.unwrap();
            let str_id = &file_name[8..(file_name.len() - 4)];
            let id_parse_result = str_id.parse::<u32>();
            if id_parse_result.is_err() {
                continue;
            }
            let id = id_parse_result.unwrap();
        }

        Ok(Self {
            current_journal: JournalManager::new(&journal_directory_path, 0, batching_param)
                .await?,
            old_journal_opt: None,
            flush_manager: FlushManager::new(memmap_arc),
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

        if self.old_journal_opt.is_some() {
            replace_with_or_abort(&mut self.old_journal_opt, |old_journal_opt| {
                let (mut old_journal, mut result_receiver) = old_journal_opt.unwrap();
                let recv_result = result_receiver.try_recv();
                if let Err(error) = recv_result {
                    return match error {
                        oneshot::error::TryRecvError::Empty => Some((old_journal, result_receiver)),
                        oneshot::error::TryRecvError::Closed => {
                            debug!("Task result channel closed");
                            None
                        }
                    };
                }
                let task_result = recv_result.unwrap();
                if task_result.is_ok() {
                    debug!("data file flushed");
                    old_journal.active_delete_on_drop();
                    None
                } else {
                    debug!("Error while try to flush data file");
                    todo!("manage error");
                    Some((old_journal, result_receiver))
                }
            });
        } else {
            if self.current_journal.journal_size() >= 64 * 1024 * 1024 {
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

                                self.old_journal_opt = Some((
                                    replace(&mut self.current_journal, task_result.1.unwrap()),
                                    self.flush_manager.flush(),
                                ));
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
        }

        finalize_result
    }
}
