//! Mapping source IDs to display order, with an explicit memory budget.
use super::sorted_runs::{ExternalSorter, SortedRecords};
use crate::{error::AppError, jobs::JobContext};
use std::{collections::HashMap, path::Path};
const RANK_BUDGET: usize = 32 * 1024 * 1024;
pub(crate) enum RowRanks {
    Dense(Vec<usize>),
    Sparse(HashMap<usize, usize>),
    External {
        records: SortedRecords,
        next: Option<(usize, usize)>,
    },
}
impl RowRanks {
    pub fn new(
        total: usize,
        ids: &[usize],
        directory: &Path,
        job: &JobContext,
    ) -> Result<Self, AppError> {
        Self::with_budget(total, ids, directory, job, RANK_BUDGET)
    }
    fn with_budget(
        total: usize,
        ids: &[usize],
        directory: &Path,
        job: &JobContext,
        budget: usize,
    ) -> Result<Self, AppError> {
        if ids.len() < total / 8 && ids.len() <= budget / 64 {
            let mut map = HashMap::with_capacity(ids.len());
            for (rank, id) in ids.iter().enumerate() {
                if rank % 1024 == 0 {
                    job.check_cancelled()?;
                }
                map.insert(*id, rank);
            }
            return Ok(Self::Sparse(map));
        }
        if total <= budget / std::mem::size_of::<usize>() {
            let mut ranks = vec![usize::MAX; total];
            for (rank, id) in ids.iter().enumerate() {
                if rank % 1024 == 0 {
                    job.check_cancelled()?;
                }
                ranks[*id] = rank;
            }
            return Ok(Self::Dense(ranks));
        }
        let mut sorter = ExternalSorter::new(directory)?;
        for (rank, id) in ids.iter().enumerate() {
            sorter.push(*id as u64, &(rank as u64).to_le_bytes(), job)?;
        }
        let mut records = sorter.finish(job)?;
        let next = records.next_rank(job)?;
        Ok(Self::External { records, next })
    }
    /// External lookup is consumed in source order, matching Parquet batches.
    pub fn get(&mut self, id: usize, job: &JobContext) -> Result<Option<usize>, AppError> {
        match self {
            Self::Dense(ranks) => Ok(ranks.get(id).copied().filter(|rank| *rank != usize::MAX)),
            Self::Sparse(ranks) => Ok(ranks.get(&id).copied()),
            Self::External { records, next } => {
                while next.is_some_and(|(source, _)| source < id) {
                    *next = records.next_rank(job)?;
                }
                if next.is_some_and(|(source, _)| source == id) {
                    let rank = next.take().map(|(_, rank)| rank);
                    *next = records.next_rank(job)?;
                    Ok(rank)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn external_rank_index_preserves_query_order_under_small_memory_budget() {
        let path = std::env::temp_dir().join(format!("trivium-ranks-{}", uuid::Uuid::new_v4()));
        let job = JobContext::none();
        let mut ranks = RowRanks::with_budget(16, &[11, 2, 7, 1], &path, &job, 64).unwrap();
        assert!(matches!(ranks, RowRanks::External { .. }));
        let found = (0..16)
            .filter_map(|id| ranks.get(id, &job).unwrap().map(|rank| (id, rank)))
            .collect::<Vec<_>>();
        assert_eq!(found, vec![(1, 3), (2, 1), (7, 2), (11, 0)]);
        drop(ranks);
        std::fs::remove_dir_all(path).unwrap();
    }
}
