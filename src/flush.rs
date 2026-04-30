use std::{io, sync::Arc, time::Duration};

use memmap2::MmapMut;
use tokio::{
    sync::mpsc,
    task::{self, JoinHandle},
    time::{Instant, timeout},
};
use tracing::{Instrument, debug, info, info_span};

use crate::{HashSetMemMap, journal::JournalManager};

pub struct FlushManager {
    task: JoinHandle<()>,
    sender: mpsc::UnboundedSender<JournalManager>,
}

impl FlushManager {
    pub fn new(hash_set_mmap: &HashSetMemMap) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let task = task::spawn(
            flush_manager_task(hash_set_mmap.create_flusher(), receiver)
                .instrument(info_span!("flush_manager_task")),
        );
        FlushManager { task, sender }
    }

    pub fn flush(&mut self, journal_manager: JournalManager) -> bool {
        self.sender.send(journal_manager).is_ok()
    }
}

async fn flush_manager_task(
    flusher: HashSetMemMapFlusher,
    mut receiver: mpsc::UnboundedReceiver<JournalManager>,
) {
    let mut journal_manager_list = Vec::with_capacity(5);
    loop {
        if journal_manager_list.is_empty() {
            receiver.recv_many(&mut journal_manager_list, 5).await;
        } else {
            let _ = timeout(
                Duration::from_secs(1),
                receiver.recv_many(&mut journal_manager_list, 5),
            )
            .await;
        }
        if journal_manager_list.is_empty() {
            info!("channel closed");
            break; //channel close
        }
        let task_flusher = flusher.clone();
        let task_result = task::spawn_blocking(move || {
            let time = Instant::now();
            let flush_result = task_flusher.flush();
            debug!("flush time: {:?}", time.elapsed());
            flush_result
        })
        .await;
        if task_result.is_err() {
            continue;
        }
        let flush_result = task_result.unwrap(); //safe unwrap
        if flush_result.is_ok() {
            while let Some(mut journal_manager) = journal_manager_list.pop() {
                journal_manager.active_delete_on_drop();
            }
        }
    }

    info!("task done")
}

#[derive(Clone)]
struct HashSetMemMapFlusher {
    memmap_arc: Arc<MmapMut>,
}
impl HashSetMemMap {
    fn create_flusher(&self) -> HashSetMemMapFlusher {
        HashSetMemMapFlusher {
            memmap_arc: self.mmap_arc.clone(),
        }
    }
}

impl HashSetMemMapFlusher {
    pub fn flush(&self) -> io::Result<()> {
        self.memmap_arc.flush()
    }
}
