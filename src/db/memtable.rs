use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
pub struct Memtable<K, V> {
    skiplist: Arc<SkipMap<K, V>>,
}

impl<K: Ord + Send + 'static, V: Send + Clone + 'static> Memtable<K, V> {
    pub fn new() -> Self {
        Memtable {
            skiplist: Arc::new(SkipMap::new()),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        self.skiplist.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.skiplist.get(key).map(|v| v.value().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_memtable() {
        let memtable = Memtable::new();

        memtable.insert(1, "hello");
        memtable.insert(2, "world");
        memtable.insert(3, "kity");

        assert_eq!(memtable.get(&1), Some("hello"));
        assert_eq!(memtable.get(&2), Some("world"));
        assert_eq!(memtable.get(&3), Some("kity"));
    }

    #[test]
    fn test_multi() {
        use std::thread;
        let memtable = Arc::new(Memtable::new());

        let mut handles = Vec::new();
        for i in 0..100 {
            let tmp = memtable.clone();
            let handle = thread::spawn(move || {
                tmp.insert(i, format!("value {}", i));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..100 {
            let lhs = memtable.get(&i);
            assert_eq!(lhs, Some(format!("value {}", i)));
        }

    }

    #[tokio::test]
    async fn test_async() {
        let memtable = Arc::new(Memtable::new());
        let mut handles = Vec::new();
        for i in 0..100 {
            let tmp = memtable.clone();
            let handle = tokio::spawn(async move {
                tmp.insert(i, format!("value {}", i));
            });
            handles.push(handle);
        }
        for handle in handles{
            handle.await.unwrap();
        }
        for i in 0..100 {
            let lhs = memtable.get(&i);
            assert_eq!(lhs, Some(format!("value {}", i)));
        }


    }
}
