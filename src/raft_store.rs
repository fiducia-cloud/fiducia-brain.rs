//! Crash-safe persistence for the brain's Raft: hard state (`current_term`,
//! `voted_for`, `commit_index`) + the replicated log.
//!
//! Raft must persist these *before* acting on them or its safety guarantees
//! break (a restarted member could vote twice in a term, or forget an entry it
//! had acknowledged). Because the brain's state machine (placement, scale plan)
//! is in memory, it is rebuilt at boot by **replaying the log up to
//! `commit_index`**, so `commit_index` has to survive too.
//!
//! This mirrors the data plane's proven atomic-write discipline (tmp → fsync →
//! rename → dir fsync; torn trailing record dropped on load) but keeps a single
//! log — the brain runs one Raft group, not a group per shard — and rewrites the
//! whole log atomically on each change. The brain's write rate is low (placement
//! decisions, not customer ops), so simplicity beats incremental append here.

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::raft::{LogEntry, Persisted};

#[derive(Debug, Default, Serialize, Deserialize)]
struct Meta {
    current_term: u64,
    voted_for: Option<String>,
    commit_index: u64,
    /// Snapshot base (0 until the log has been compacted at least once).
    #[serde(default)]
    base_index: u64,
    #[serde(default)]
    base_term: u64,
}

/// A brain member's durable Raft home under `<dir>/`: a `meta` file and a `log`.
pub struct RaftStore {
    dir: PathBuf,
    meta_path: PathBuf,
    log_path: PathBuf,
}

impl RaftStore {
    /// Open (creating if needed) the store under `dir`, returning it alongside the
    /// [`Persisted`] state recovered from disk (all-zero / empty for a fresh
    /// member). The log is canonicalized on open: any torn trailing record (crash
    /// mid-write) is dropped and the file rewritten from clean bytes.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<(Self, Persisted)> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let meta_path = dir.join("meta");
        let log_path = dir.join("log");

        let meta: Meta = match fs::read(&meta_path) {
            Ok(bytes) if !bytes.is_empty() => serde_json::from_slice(&bytes).map_err(invalid)?,
            _ => Meta::default(),
        };

        // Parse every complete JSON line; stop at the first that fails — that's a
        // record torn by a crash mid-write, and everything after it.
        let mut log: Vec<LogEntry> = Vec::new();
        if let Ok(file) = File::open(&log_path) {
            for line in BufReader::new(file).split(b'\n') {
                let line = line?;
                if line.is_empty() {
                    continue;
                }
                match serde_json::from_slice::<LogEntry>(&line) {
                    Ok(entry) => log.push(entry),
                    Err(_) => break,
                }
            }
        }

        let store = RaftStore {
            dir,
            meta_path,
            log_path,
        };
        // Canonicalize on disk so the next write starts from clean bytes.
        store.write_log(&log)?;

        let restored = Persisted {
            current_term: meta.current_term,
            voted_for: meta.voted_for,
            commit_index: meta.commit_index,
            log,
        };
        Ok((store, restored))
    }

    /// Durably save hard state + log. The log is written **first**, then meta, so
    /// `commit_index` can never reference entries that aren't yet on disk.
    pub fn save(&self, p: &Persisted) -> io::Result<()> {
        self.write_log(&p.log)?;
        self.write_meta(&Meta {
            current_term: p.current_term,
            voted_for: p.voted_for.clone(),
            commit_index: p.commit_index,
        })
    }

    fn write_meta(&self, meta: &Meta) -> io::Result<()> {
        let bytes = serde_json::to_vec(meta).map_err(invalid)?;
        atomic_write(&self.meta_path, &bytes, &self.dir)
    }

    fn write_log(&self, log: &[LogEntry]) -> io::Result<()> {
        let mut buf = Vec::new();
        for entry in log {
            serde_json::to_writer(&mut buf, entry).map_err(invalid)?;
            buf.push(b'\n');
        }
        atomic_write(&self.log_path, &buf, &self.dir)
    }
}

fn invalid<E>(e: E) -> io::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// Write `bytes` to `path` atomically: tmp file → fsync → rename → fsync(dir).
fn atomic_write(path: &Path, bytes: &[u8], dir: &Path) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::Command;

    fn entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            command: Some(Command::ForgetNode(format!("n{index}"))),
        }
    }

    fn tmpdir() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "fiducia-brain-raft-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn fresh_store_recovers_empty() {
        let (_s, rec) = RaftStore::open(tmpdir()).unwrap();
        assert_eq!(rec.current_term, 0);
        assert_eq!(rec.voted_for, None);
        assert_eq!(rec.commit_index, 0);
        assert!(rec.log.is_empty());
    }

    #[test]
    fn hard_state_and_log_round_trip_across_reopen() {
        let dir = tmpdir();
        {
            let (store, _) = RaftStore::open(&dir).unwrap();
            store
                .save(&Persisted {
                    current_term: 7,
                    voted_for: Some("brain-b".to_string()),
                    commit_index: 2,
                    log: vec![entry(1, 7), entry(2, 7)],
                })
                .unwrap();
        }
        let (_s, rec) = RaftStore::open(&dir).unwrap();
        assert_eq!(rec.current_term, 7);
        assert_eq!(rec.voted_for.as_deref(), Some("brain-b"));
        assert_eq!(rec.commit_index, 2);
        assert_eq!(rec.log.len(), 2);
        assert_eq!(rec.log[1].index, 2);
    }

    #[test]
    fn torn_trailing_record_is_dropped_on_load() {
        let dir = tmpdir();
        {
            let (store, _) = RaftStore::open(&dir).unwrap();
            store
                .save(&Persisted {
                    current_term: 1,
                    voted_for: None,
                    commit_index: 1,
                    log: vec![entry(1, 1)],
                })
                .unwrap();
        }
        // Simulate a crash mid-write: append a partial JSON line with no newline.
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(dir.join("log"))
            .unwrap();
        f.write_all(b"{\"term\":1,\"index\":2,\"comm").unwrap();
        f.sync_all().unwrap();
        drop(f);

        let (_s, rec) = RaftStore::open(&dir).unwrap();
        assert_eq!(rec.log.len(), 1, "torn record must be dropped");
        assert_eq!(rec.log[0].index, 1);
    }
}
