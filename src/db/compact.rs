use super::level::*;
use crate::table::table::Table;
use crate::utils::slice::Slice;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::time::{Duration, SystemTime};

#[derive(Clone)]
struct CompactionPriority {
    level: u32,
    score: f64,
    adjusted: f64,
    t: Targets,
}

#[derive(Clone)]
struct Targets {
    base_level: u32,
    target_sz: Vec<u64>,
    file_sz: Vec<u64>,
}

struct CompactDef {
    compact_id: u32,
    t: Targets,
    p: CompactionPriority,
    this_level: u32,
    next_level: u32,
    this_sz: u64,
    tables: Vec<u64>,

    top: Vec<u32>,
    bot: Vec<u32>,

    this_range: KeyRange,
    next_range: KeyRange,
}

#[derive(Default)]
pub(crate) struct CompactStatus {
    levels: Vec<LevelCompactStatus>,
    tables: HashSet<u64>,
}

// level compact status
pub(crate) struct LevelCompactStatus {
    ranges: Vec<KeyRange>,
    del_sz: u64,
}
#[derive(Clone)]
struct KeyRange {
    left: Slice,
    right: Slice,
}

impl LevelManager {
    fn level_targets(&self) -> Result<Targets, String> {
        let adjust = |sz: u64| std::cmp::max(sz, self.opt.base_level_size);

        let mut t = Targets {
            base_level: 0,
            target_sz: vec![0; self.levels.len()],
            file_sz: vec![0; self.levels.len()],
        };

        // compute from last level
        let len = self.levels.len();

        let mut db_size = self.get_level_total_size(len - 1);
        for i in (1..len).rev() {
            let level_target_sz = adjust(db_size);
            t.target_sz[i] = level_target_sz;
            // if tmp level not reach expect sz
            if t.base_level == 0 && level_target_sz <= self.opt.base_level_size {
                t.base_level = i as u32;
            }
            db_size /= self.opt.level_size_multiplier as u64;
        }

        let mut tsz = self.opt.base_table_size;
        for i in 0..len {
            if i == 0 {
                t.file_sz[i] = self.opt.memtable_size;
            } else if i <= t.base_level as usize {
                t.file_sz[i] = tsz;
            } else {
                tsz *= self.opt.table_size_multiplier as u64;
                t.file_sz[i] = tsz;
            }
        }

        // find last empty level
        for i in (t.base_level as usize + 1)..len {
            if self.get_level_total_size(i) > 0 {
                break;
            }
            t.base_level = i as u32;
        }

        Ok(t)
    }

    // selects an appropriate level to perform a compaction
    // and returns the priority of the decision
    fn pick_compact_levels(&self) -> Result<Vec<CompactionPriority>, String> {
        let t = self.level_targets()?;

        let mut prios = Vec::new();
        let mut add_priority = |level: u32, score: f64| {
            let pri = CompactionPriority {
                level,
                score,
                adjusted: score,
                t: t.clone(),
            };
            prios.push(pri);
        };

        // adjust the compression priority based on
        // the number of tables in the l0 level

        add_priority(
            0,
            self.get_level_num_tables(0) as f64 / self.opt.num_level_zero_tables as f64,
        );
        let level_0 = self.levels[0].read().map_err(|e| e.to_string())?;

        // Non-l0 levels calculate priority based on size
        let len = self.levels.len();
        for i in 1..len {
            // SSTs in a compression state cannot be included in the calculation
            let del_sz = self.get_compact_delsize(i);
            let sz = self.get_level_total_size(i) - del_sz;
            // size / expected size
            add_priority(i as u32, sz as f64 / t.target_sz[i] as f64);
        }

        assert!(prios.len() == len);

        // todo! adjust score
        let mut out = Vec::new();
        for p in prios {
            if p.score >= 1.0 {
                out.push(p);
            }
        }

        out.sort_by(|i, j| i.adjusted.partial_cmp(&j.adjusted).unwrap());

        Ok(out)
    }

    fn move_l0_to_front(prios: Vec<CompactionPriority>) -> Vec<CompactionPriority> {
        let mut idx: i32 = -1;
        for (i, p) in prios.iter().enumerate() {
            if p.level == 0 {
                idx = i as i32;
                break;
            }
        }

        // if idx == -1, we didn't find L0
        // if idx == 0, then we don't need to do anything, L0 is already at the front
        if idx > 0 {
            let mut v = Vec::new();
            v.push(prios[idx as usize].clone());
            v.extend_from_slice(&prios[0..idx as usize]);
            v.extend_from_slice(&prios[idx as usize..]);
            return v;
        }
        prios
    }

    // L0 to L0 table compression
    fn fill_tables_l0_to_l0(&self, cd: &mut CompactDef) -> Result<(), String> {
        if cd.compact_id != 0 {
            return Err("Only the 0th compression processor can excute, to avoid resource contention in L0 to L0 compression ".to_string());
        }

        cd.next_level = 0;

        let tables = &self.levels[0].read().unwrap().tables;
        let now = SystemTime::now();
        let mut out = Vec::new();
        for (i, table) in tables.iter().enumerate() {
            if table.size() >= 2 * cd.t.file_sz[0] {
                // avoid compressing sst files that are too large, as this can
                // cause performance jitter
                continue;
            }

            // if sst created time less than 10s, ignore
            let dur = now.duration_since(table.create_at()).unwrap();
            if dur < Duration::from_secs(10) {
                continue;
            }

            // if sst is in compressing state, ignore
            let cs = self.compact_state.read().unwrap();
            if cs.tables.contains(&table.id().unwrap()) {
                continue;
            }
            out.push(i);
        }

        if out.len() < 4 {
            // not compressing if the number of ssts that meet the condition is less than 4
            return Err("too few files".to_string());
        }

        let mut cs = self.compact_state.write().unwrap();
        for idx in out {
            cs.tables.insert(tables[idx].id().unwrap());
        }

        cd.t.file_sz[0] = u64::MAX;
        Ok(())
    }

    fn fill_tables_l0_to_base(&mut self, cd: &mut CompactDef) -> Result<(), String> {
        if cd.next_level == 0 {
            return Err("base level cannot be zero".to_string());
        }

        // if priority is less than 1, not executed{
        if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
            return Err("adjusted score is less than 1.0".to_string());
        }

        let top = &self.levels[cd.this_level as usize].read().unwrap().tables;

        if top.len() == 0 {
            return Err("top level empty".to_string());
        }

        let mut out = Vec::new();
        let mut kr = KeyRange::new();
        // cd.top[0] is the oldest file, start from the oldest file
        for (i, table) in top.iter().enumerate() {
            let dkr = KeyRange::with_table(table);
            if kr.overlap_with(&dkr) {
                out.push(i as u32);
                kr.extend(dkr);
                cd.tables.push(table.id().unwrap());
            } else {
                // terminal if any range not overlap
                break;
            }
        }

        let (left, right) = self.get_level_overlapping_tables(cd.next_level as usize, &kr)?;
        cd.top = out;
        cd.this_range = kr;
        let v: Vec<u32> = (left as u32..=right as u32).collect();

        let bot = &self.levels[cd.next_level as usize].read().unwrap().tables;
        let bot: Vec<&Table> = v.iter().map(|&i| &bot[i as usize]).collect();
        cd.bot = v;
        cd.next_range = KeyRange::with_tables(&bot);
        for table in bot {
            cd.this_sz += table.size();
            cd.tables.push(table.id().unwrap());
        }

        self.compact_state.write().unwrap().compare_and_add(&cd)
    }

    // returns the tables that intersect with key range
    fn get_level_overlapping_tables(
        &self,
        idx: usize,
        kr: &KeyRange,
    ) -> Result<(usize, usize), String> {
        if kr.left.is_empty() || kr.right.is_empty() {
            return Err("kr is empty".to_string());
        }
        let level = self.levels[idx].read().unwrap();
        let len = level.tables.len();
        let v: Vec<usize> = (0..len).collect();

        let left = v
            .binary_search_by(|&i| kr.left.cmp(level.tables[i].max_key()))
            .map_err(|e| format!("kr.left > max key of tables, {}", e))?;
        let right = v.binary_search_by(|&i| kr.right.cmp(level.tables[i].max_key()));

        if let Ok(r) = right {
            Ok((left, r))
        } else {
            Ok((left, len - 1))
        }
    }

    fn get_compact_delsize(&self, idx: usize) -> u64 {
        self.compact_state.read().unwrap().levels[idx].del_sz
    }
}

impl KeyRange {
    pub fn new() -> Self {
        KeyRange {
            left: Vec::new(),
            right: Vec::new(),
        }
    }
    pub fn with_table(table: &Table) -> Self {
        KeyRange {
            left: (*table.min_key()).clone(),
            right: (*table.max_key()).clone(),
        }
    }

    pub fn with_tables(tables: &Vec<&Table>) -> Self {
        if tables.is_empty() {
            return KeyRange::new();
        }
        let mut min_key = tables[0].min_key();
        let mut max_key = tables[1].max_key();
        for table in tables {
            if table.min_key().cmp(min_key) == Ordering::Less {
                min_key = table.min_key();
            }
            if table.max_key().cmp(max_key) == Ordering::Greater {
                max_key = table.max_key();
            }
        }
        KeyRange {
            left: (*min_key).clone(),
            right: (*max_key).clone(),
        }
    }
    pub fn overlap_with(&self, dst: &KeyRange) -> bool {
        // empty keyrange alaways overlaps
        if self.is_empty() {
            return true;
        }

        // empty dst doesn't overlap with anything
        if dst.is_empty() {
            return false;
        }

        if self.left.cmp(&dst.right) == Ordering::Greater {
            return false;
        }

        if self.right.cmp(&dst.left) == Ordering::Less {
            return false;
        }

        true
    }

    pub fn extend(&mut self, kr: KeyRange) {
        if kr.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = kr;
            return;
        }
        if self.left.is_empty() || self.left.cmp(&kr.left) == Ordering::Greater {
            self.left = kr.left;
        }
        if self.right.is_empty() || self.right.cmp(&kr.right) == Ordering::Less {
            self.right = kr.right;
        }
    }

    fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty()
    }
}

impl CompactStatus {
    fn compare_and_add(&mut self, cd: &CompactDef) -> Result<(), String> {
        {
            let this_level = &self.levels[cd.this_level as usize];
            let next_level = &self.levels[cd.next_level as usize];

            if this_level.overlap_with(&cd.this_range) {
                return Err("not overlap".to_string());
            }
            if next_level.overlap_with(&cd.next_range) {
                return Err("not overlap".to_string());
            }
        }
        {
            let this_level = &mut self.levels[cd.this_level as usize];

            this_level.ranges.push(cd.this_range.clone());
            this_level.del_sz += cd.this_sz;
        }

        let next_level = &mut self.levels[cd.next_level as usize];
        next_level.ranges.push(cd.next_range.clone());
        for id in &cd.tables {
            self.tables.insert(*id);
        }
        Ok(())
    }
}

impl LevelCompactStatus {
    fn overlap_with(&self, dst: &KeyRange) -> bool {
        for r in &self.ranges {
            if r.overlap_with(&dst) {
                return true;
            }
        }
        false
    }
}
