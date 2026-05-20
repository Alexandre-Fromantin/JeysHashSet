use std::{io, path::Path, time::Duration};

use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout},
};

use crate::{
    HashSet,
    batching::{BatchingAction, BatchingParameter},
};

struct BatchingStrategy {
    max_timeout: Duration,
    min_timeout: Duration,
    min_batch_size: usize,
    default_batch_size: usize,
    max_batch_size: usize,
}

struct BatchElement {
    batching_action: BatchingAction,
    success_opt: Option<bool>,
    responder: oneshot::Sender<bool>,
}

struct TaskWorker {
    batch_element_receiver: mpsc::Receiver<BatchElement>,
    hashset: HashSet,
    batching_strategy: BatchingStrategy,
    batch: Vec<BatchElement>,
}

struct BatchIterator<'a> {
    iter_index: usize,
    batch: &'a mut [BatchElement],
}

impl<'a> BatchIterator<'a> {
    pub fn from_vec(vec: &'a mut Vec<BatchElement>) -> Self {
        Self {
            iter_index: 0,
            batch: vec,
        }
    }
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = (BatchingAction, &'a mut Option<bool>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter_index < self.batch.len() {
            unsafe {
                let element_ptr = self.batch.as_mut_ptr().add(self.iter_index);
                self.iter_index += 1;
                Some((
                    (*element_ptr).batching_action,
                    &mut (*element_ptr).success_opt,
                ))
            }
        } else {
            None
        }
    }
}

impl TaskWorker {
    pub fn create(
        hashset: HashSet,
        batching_strategy: BatchingStrategy,
    ) -> mpsc::Sender<BatchElement> {
        let (order_sender, order_receiver) = mpsc::channel(10240);
        let mut manager_task = TaskWorker {
            batch: Vec::with_capacity(batching_strategy.default_batch_size),
            batch_element_receiver: order_receiver,
            hashset,
            batching_strategy,
        };
        tokio::spawn(async move { manager_task.run().await });

        order_sender
    }

    async fn run(&mut self) {
        loop {
            //for each batch
            let recv_amount = self
                .batch_element_receiver
                .recv_many(&mut self.batch, self.batching_strategy.max_batch_size)
                .await;
            if recv_amount == 0 {
                //channel close
                break;
            };

            let mut current_instant = Instant::now();
            let next_batching_min_instant = current_instant + self.batching_strategy.min_timeout;
            let next_batching_max_instant = current_instant + self.batching_strategy.max_timeout;

            loop {
                //for each n-element for batch
                let current_batch_size = self.batch.len();
                let target_instant = if next_batching_min_instant > current_instant {
                    next_batching_min_instant
                } else {
                    next_batching_max_instant
                };
                let _ = timeout(
                    target_instant - current_instant,
                    self.batch_element_receiver.recv_many(
                        &mut self.batch,
                        self.batching_strategy.max_batch_size - current_batch_size,
                    ),
                )
                .await;

                current_instant = Instant::now();
                if next_batching_max_instant <= current_instant//MAX timeout
                || self.batch.len() >= self.batching_strategy.max_batch_size//batch full
                || (self.batch.len() >= self.batching_strategy.default_batch_size//after MIN timeout
                    && next_batching_min_instant <= current_instant)
                {
                    self.hashset
                        .batch(BatchIterator::from_vec(&mut self.batch))
                        .await;

                    for element in self.batch.drain(..) {
                        if let Some(success) = element.success_opt {
                            let _ = element.responder.send(success);
                        }
                    }

                    break; //start a new batch
                }
            }
        }
    }
}

#[derive(Clone)]
struct HashSetManager {
    batch_element_sender: mpsc::Sender<BatchElement>,
}

pub enum HashSetManagerError {
    IOError(io::Error),
}

impl From<io::Error> for HashSetManagerError {
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}

impl HashSetManager {
    pub async fn open(
        directory_path: &Path,
        batching_strategy: BatchingStrategy,
    ) -> Result<Self, HashSetManagerError> {
        let hashset = HashSet::from_file(
            directory_path,
            BatchingParameter {
                pre_allocated_size: batching_strategy.default_batch_size,
            },
        )
        .await?;

        let batch_element_sender = TaskWorker::create(hashset, batching_strategy);

        Ok(Self {
            batch_element_sender,
        })
    }
}
