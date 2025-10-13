
use std::collections::BTreeSet;

use frame::Frame;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use std::time::Instant;

use bytes::Bytes;

use std::collections::HashMap;

use std::sync::Mutex;

use std::sync::Arc;

use crate::frame;

pub type DB = Arc<Mutex<HashMap<String, (Bytes, Option<Instant>)>>>;

pub type Streams = Arc<Mutex<HashMap<String, Vec<((u128, u64), Vec<(String, Bytes)>)>>>>;

pub type StreamsTx = broadcast::Sender<(String, Option<Vec<((u128, u64), Vec<(String, Bytes)>)>>)>;

pub type Lists = Arc<Mutex<HashMap<String, Vec<String>>>>;

pub type WaitLists = Arc<Mutex<HashMap<String, Vec<oneshot::Sender<String>>>>>;

pub type Zsets = Arc<Mutex<HashMap<String, SortedSet>>>;

#[derive(Default)]
pub struct SortedSet {
    pub entries: Vec<(f64, String)>,
}

impl SortedSet {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn insert(&mut self, entry: (f64, String)) -> bool {
        let pos = self.entries.iter().position(|e| e.1 == entry.1);
        if let Some(pos) = pos {
            if entry.0 != self.entries[pos].0 {
                self.entries[pos] = entry;
                self.entries.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then_with(|| a.1.cmp(&b.1)));
            }
            false 
        } else {
            self.entries.push(entry);
            self.entries.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then_with(|| a.1.cmp(&b.1)));
            true
        }
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        self.entries.iter().position(|e| e.1 == member)
    }

    pub fn range(&self, start: isize, stop: isize) -> Vec<(f64, String)> {
        let len = self.entries.len() as isize;
        let start = if start < 0 { (len + start).max(0) } else { start.min(len) };
        let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len) };
        if start > stop || start >= len {
            return vec![];
        }
        self.entries[start as usize..=stop as usize].to_vec()
    }

    pub fn card(&self) -> usize {
        self.entries.len()
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        self.entries.iter().find(|e| e.1 == member).map(|e| e.0)
    }

    pub fn remove(&mut self, member: &str) -> bool {
        let pos = self.entries.iter().position(|(_, m)| m == member);
        if let Some(pos) = pos  {
            self.entries.remove(pos);
            true
        } else {
            false
        }
    }
}

#[test]
fn test_sorted_set_insert() {
    let mut zset = SortedSet::new();
    assert!(zset.insert((1.0, "a".to_string())));
    assert!(zset.insert((2.0, "c".to_string())));
    assert!(zset.insert((2.0, "b".to_string())));
    assert!(!zset.insert((1.5, "a".to_string())));
    assert_eq!(zset.entries, vec![(1.5, "a".to_string()), (2.0, "b".to_string()), (2.0, "c".to_string())]);
}

#[derive(Clone)]
pub struct Env {
    pub db: DB,
    pub config: Arc<Mutex<HashMap<String, String>>>,
    pub streams: Streams,
    pub tx: broadcast::Sender<(Frame, Option<mpsc::Sender<u64>>)>,
    pub streams_tx: StreamsTx,
    pub lists: Lists,
    pub wait_lists: WaitLists,
    pub zsets: Zsets,
}

impl Env {
    pub fn new() -> Self {
        let db: DB = Arc::new(Mutex::new(HashMap::new()));
        let config = Arc::new(Mutex::new(HashMap::new()));
        let streams = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _) = broadcast::channel(32);
        let (streams_tx, _) = broadcast::channel(32);
        let lists = Arc::new(Mutex::new(HashMap::new()));
        let wait_lists = Arc::new(Mutex::new(HashMap::new()));
        let zsets = Arc::new(Mutex::new(HashMap::new()));
        Self {
            db,
            config,
            streams,
            tx,
            streams_tx,
            lists,
            wait_lists,
            zsets,
        }
    }
}
