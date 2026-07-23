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
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

use crate::raft::{LogEntry, Persisted};

#[derive(Debug, Default, Serialize, Deserialize)]
struct Meta {
    current_term: u64,
    voted_for: Option<String>,
    commit_index: u64,
    /// Snapshot base (0 until the log has been compacted at least once). Kept for
    /// compatibility with logs written before the header existed (format v0); the
    /// header in the log file itself is authoritative when present.
    #[serde(default)]
    base_index: u64,
    #[serde(default)]
    base_term: u64,
}

/// Current log file format. v0 (no header, entries only) is still readable.
const LOG_FORMAT_VERSION: u64 = 1;

/// First line of the log file: the snapshot base the entries continue from.
///
/// The base lives *in the log file* because the two must agree: they used to be
/// two independent atomic writes (log, then meta), so a crash between them left a
/// log starting at 257 next to a meta saying `base_index = 0` — a mismatch
/// [`validate_recovered`] rejects, i.e. an unbootable member. One rename now
/// carries both. `v` distinguishes a header from a v0 log's first [`LogEntry`],
/// which has no such field.
#[derive(Debug, Serialize, Deserialize)]
struct LogHeader {
    v: u64,
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
    snapshot_path: PathBuf,
    #[cfg(test)]
    fail_saves: AtomicBool,
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
        let snapshot_path = dir.join("snapshot");

        let meta: Meta = match fs::read(&meta_path) {
            Ok(bytes) if !bytes.is_empty() => serde_json::from_slice(&bytes).map_err(invalid)?,
            Ok(_) => Meta::default(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Meta::default(),
            Err(err) => return Err(err),
        };

        // The state-machine snapshot, present once the log has been compacted.
        let snapshot = match fs::read(&snapshot_path) {
            Ok(bytes) if !bytes.is_empty() => Some(bytes),
            Ok(_) => None,
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(err),
        };

        // Parse every complete JSON line; stop at the first that fails — that's a
        // record torn by a crash mid-write, and everything after it. A v1 log
        // opens with its header; a v0 log (written before the header existed)
        // starts straight at the first entry and takes its base from meta.
        let mut header: Option<LogHeader> = None;
        let mut log: Vec<LogEntry> = Vec::new();
        match File::open(&log_path) {
            Ok(file) => {
                for (n, line) in BufReader::new(file).split(b'\n').enumerate() {
                    let line = line?;
                    if line.is_empty() {
                        continue;
                    }
                    if n == 0 {
                        if let Ok(parsed) = serde_json::from_slice::<LogHeader>(&line) {
                            header = Some(parsed);
                            continue;
                        }
                    }
                    match serde_json::from_slice::<LogEntry>(&line) {
                        Ok(entry) => log.push(entry),
                        Err(_) => break,
                    }
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        if let Some(header) = &header {
            if header.v > LOG_FORMAT_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Raft log format v{} is newer than this binary understands (v{LOG_FORMAT_VERSION})",
                        header.v
                    ),
                ));
            }
        }
        let (base_index, base_term) = match &header {
            Some(h) => (h.base_index, h.base_term),
            None => (meta.base_index, meta.base_term),
        };
        // A crash between the log and meta writes can leave meta a step behind.
        // The snapshot beside a non-zero base only ever holds *committed* state
        // (compaction refuses to fold in anything above `commit_index`), so
        // recovering `commit_index` as at least `base_index` restores what was
        // durably true rather than inventing it.
        let commit_index = meta.commit_index.max(base_index);

        let store = RaftStore {
            dir,
            meta_path,
            log_path,
            snapshot_path,
            #[cfg(test)]
            fail_saves: AtomicBool::new(false),
        };
        validate_recovered(commit_index, base_index, snapshot.as_deref(), &log)?;
        // Canonicalize on disk so the next write starts from clean bytes.
        store.write_log(base_index, base_term, &log)?;

        let restored = Persisted {
            current_term: meta.current_term,
            voted_for: meta.voted_for,
            commit_index,
            log,
            base_index,
            base_term,
            snapshot,
        };
        Ok((store, restored))
    }

    /// Durably save hard state + log. The log is written **first**, then meta, so
    /// `commit_index` can never reference entries that aren't yet on disk.
    pub fn save(&self, p: &Persisted) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_saves.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected RaftStore save failure"));
        }
        // Snapshot first, then log, then meta — so the `base_index`/`commit_index`
        // recorded in meta can never reference a snapshot or entries not yet on disk.
        // The log's header carries the base alongside the entries it belongs to, so
        // that pair always lands in a single rename.
        if let Some(snapshot) = &p.snapshot {
            atomic_write(&self.snapshot_path, snapshot, &self.dir)?;
        }
        self.write_log(p.base_index, p.base_term, &p.log)?;
        self.write_meta(&Meta {
            current_term: p.current_term,
            voted_for: p.voted_for.clone(),
            commit_index: p.commit_index,
            base_index: p.base_index,
            base_term: p.base_term,
        })
    }

    #[cfg(test)]
    pub fn fail_saves_for_test(&self) {
        self.fail_saves.store(true, Ordering::SeqCst);
    }

    fn write_meta(&self, meta: &Meta) -> io::Result<()> {
        let bytes = serde_json::to_vec(meta).map_err(invalid)?;
        atomic_write(&self.meta_path, &bytes, &self.dir)
    }

    fn write_log(&self, base_index: u64, base_term: u64, log: &[LogEntry]) -> io::Result<()> {
        let mut buf = Vec::new();
        serde_json::to_writer(
            &mut buf,
            &LogHeader {
                v: LOG_FORMAT_VERSION,
                base_index,
                base_term,
            },
        )
        .map_err(invalid)?;
        buf.push(b'\n');
        for entry in log {
            serde_json::to_writer(&mut buf, entry).map_err(invalid)?;
            buf.push(b'\n');
        }
        atomic_write(&self.log_path, &buf, &self.dir)
    }
}

/// Reject inconsistent durable state before canonicalizing a torn log. In
/// particular, recovery may discard only an uncommitted torn tail; it must
/// never erase evidence of an entry referenced by the durable commit index.
fn validate_recovered(
    commit_index: u64,
    base_index: u64,
    snapshot: Option<&[u8]>,
    log: &[LogEntry],
) -> io::Result<()> {
    if base_index > 0 && snapshot.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft snapshot is missing for non-zero base_index",
        ));
    }
    let mut expected = base_index.saturating_add(1);
    for entry in log {
        if entry.index != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Raft log is not contiguous: expected index {expected}, found {}",
                    entry.index
                ),
            ));
        }
        expected = expected.saturating_add(1);
    }
    let last_index = log.last().map(|entry| entry.index).unwrap_or(base_index);
    if commit_index > last_index {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Raft commit_index {commit_index} exceeds last recoverable index {last_index}; refusing to truncate committed entries"
            ),
        ));
    }
    Ok(())
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
        // A per-process counter, not just the clock: the clock's resolution is
        // coarse enough that two tests starting together shared a directory (and
        // each other's files).
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "fiducia-brain-raft-{}-{:?}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQ.fetch_add(1, Ordering::SeqCst)
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
                    ..Default::default()
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
                    ..Default::default()
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

    /// Regression (F11): the log header makes the snapshot base and the entries it
    /// belongs to land in ONE rename. A crash after the log write but before the
    /// meta write used to leave `base_index = 0` beside a log starting at 4 —
    /// recovery rejected that as non-contiguous and the member was unbootable.
    #[test]
    fn a_crash_between_the_log_and_meta_writes_still_recovers() {
        let dir = tmpdir();
        let (store, _) = RaftStore::open(&dir).unwrap();
        // The state as it was before compaction, which is what meta still holds.
        store
            .save(&Persisted {
                current_term: 2,
                voted_for: None,
                commit_index: 3,
                log: vec![entry(1, 2), entry(2, 2), entry(3, 2)],
                ..Default::default()
            })
            .unwrap();
        // Compaction: snapshot + log (with the new base in its header) reach disk,
        // then the process dies before meta is rewritten.
        fs::write(dir.join("snapshot"), b"state@3").unwrap();
        store.write_log(3, 2, &[entry(4, 2)]).unwrap();

        let (_s, rec) = RaftStore::open(&dir).expect("a half-finished save still boots");
        assert_eq!(rec.base_index, 3, "base recovered from the log header");
        assert_eq!(rec.base_term, 2);
        assert_eq!(
            rec.commit_index, 3,
            "commit cannot precede the snapshot base"
        );
        assert_eq!(rec.log.len(), 1);
        assert_eq!(rec.log[0].index, 4);
        assert_eq!(rec.snapshot.as_deref(), Some(b"state@3".as_slice()));
    }

    /// A log written by a previous build (v0: entries only, base in meta) must
    /// still open — and is rewritten in the versioned format on the way through.
    #[test]
    fn a_headerless_v0_log_is_read_and_migrated() {
        let dir = tmpdir();
        let (store, _) = RaftStore::open(&dir).unwrap();
        store
            .write_meta(&Meta {
                current_term: 4,
                voted_for: Some("brain-c".to_string()),
                commit_index: 5,
                base_index: 3,
                base_term: 2,
            })
            .unwrap();
        fs::write(dir.join("snapshot"), b"state@3").unwrap();
        // v0 log bytes: no header line, entries continuing from base_index.
        let mut buf = Vec::new();
        for e in [entry(4, 4), entry(5, 4)] {
            serde_json::to_writer(&mut buf, &e).unwrap();
            buf.push(b'\n');
        }
        fs::write(dir.join("log"), &buf).unwrap();

        let (_s, rec) = RaftStore::open(&dir).expect("v0 log still opens");
        assert_eq!(rec.base_index, 3, "base taken from meta for a v0 log");
        assert_eq!(rec.base_term, 2);
        assert_eq!(rec.commit_index, 5);
        assert_eq!(rec.log.len(), 2);
        assert_eq!(rec.voted_for.as_deref(), Some("brain-c"));

        // Canonicalization rewrote it with a header, and it round-trips.
        let migrated = fs::read(dir.join("log")).unwrap();
        assert!(migrated.starts_with(b"{\"v\":1"), "migrated to v1");
        let (_s, again) = RaftStore::open(&dir).unwrap();
        assert_eq!(again.base_index, 3);
        assert_eq!(again.log.len(), 2);
    }

    #[test]
    fn torn_committed_record_aborts_recovery_without_canonicalizing_log() {
        let dir = tmpdir();
        let (store, _) = RaftStore::open(&dir).unwrap();
        store
            .write_meta(&Meta {
                current_term: 2,
                voted_for: None,
                commit_index: 2,
                base_index: 0,
                base_term: 0,
            })
            .unwrap();
        store.write_log(0, 0, &[entry(1, 2)]).unwrap();
        let torn = b"{\"term\":2,\"index\":2,\"comm";
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(dir.join("log"))
            .unwrap();
        f.write_all(torn).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let before = fs::read(dir.join("log")).unwrap();
        let err = RaftStore::open(&dir)
            .err()
            .expect("recovery must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err
            .to_string()
            .contains("refusing to truncate committed entries"));
        assert_eq!(
            fs::read(dir.join("log")).unwrap(),
            before,
            "failed recovery must preserve the torn WAL for diagnosis"
        );
    }
}
