use super::iterator::*;
use super::level::*;
use super::options::Options;
use crate::file::manifest::*;
use crate::pb::pb::{ManifestChange, ManifestChangeSet};
use crate::table::table::Table;
use crate::table::table_builder::TableBuilder;
use crate::utils::file::file_helper;
use crate::utils::slice::Slice;
use prost::Message;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};

#[derive(Clone, Debug)]
struct CompactionPriority {
    level: u32,
    score: f64,
    adjusted: f64,
    t: Targets,
}

#[derive(Clone, Debug)]
struct Targets {
    base_level: u32,
    target_sz: Vec<u64>,
    file_sz: Vec<u64>,
}

#[derive(Clone, Debug)]
struct CompactDef {
    compact_id: u32,
    t: Targets,
    this_level: u32,
    p: CompactionPriority,
    next_level: u32,
    this_sz: u64,
    tables: Vec<u64>,
    splits: Vec<KeyRange>,

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
#[derive(Default)]
pub(crate) struct LevelCompactStatus {
    ranges: Vec<KeyRange>,
    del_sz: u64,
}
#[derive(Clone, Debug)]
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

    fn fill_tables_l0_to_base(&self, cd: &mut CompactDef) -> Result<(), String> {
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

        cd.top = out;
        cd.this_range = kr;
        if let Ok((left, right)) =
            self.get_level_overlapping_tables(cd.next_level as usize, &cd.this_range)
        {
            let v: Vec<u32> = (left as u32..=right as u32).collect();

            let bot = &self.levels[cd.next_level as usize].read().unwrap().tables;
            let bot: Vec<&Table> = v.iter().map(|&i| &bot[i as usize]).collect();
            cd.bot = v;
            cd.next_range = KeyRange::with_tables(&bot);
            for table in bot {
                cd.this_sz += table.size();
                cd.tables.push(table.id().unwrap());
            }
        }

        self.compact_state.write().unwrap().compare_and_add(&cd)
    }

    fn fill_tables(&self, cd: &mut CompactDef) -> Result<(), String> {
        let tables = &self.levels[cd.this_level as usize].read().unwrap().tables;
        if tables.len() == 0 {
            return Err("this level is empty".to_string());
        }

        for (i, table) in tables.iter().enumerate() {
            cd.this_sz = table.size();
            cd.this_range = KeyRange::with_table(table);
            // do nothing if has been compressing
            {
                if self.compact_state_overlap_with(cd.this_level as usize, &cd.this_range) == true {
                    continue;
                }
            }
            if let Ok((left, right)) =
                self.get_level_overlapping_tables(cd.next_level as usize, &cd.this_range)
            {
                let v: Vec<u32> = (left..=right).map(|i| i as u32).collect();
                let bot = &self.levels[cd.next_level as usize].read().unwrap().tables;
                let bot: Vec<&Table> = v.iter().map(|&i| &bot[i as usize]).collect();

                cd.bot = v;
                cd.next_range = KeyRange::with_tables(&bot);
                for table in bot {
                    cd.this_sz += table.size();
                    cd.tables.push(table.id().unwrap());
                }
                if let Ok(()) = self.compact_state.write().unwrap().compare_and_add(&cd) {
                    return Ok(());
                } else {
                    continue;
                }
            }
        }

        Err("no overlap".to_string())
    }

    // fill_tables_l0 first try L0 to L_base compressing, if failed
    // compressing L0 to L0.
    fn fill_tables_l0(&self, cd: &mut CompactDef) -> Result<(), String> {
        if let Ok(()) = self.fill_tables_l0_to_base(cd) {
            Ok(())
        } else {
            self.fill_tables_l0_to_l0(cd)
        }
    }

    // parallel execution of sub-compression scenarios
    fn add_splits(&self, cd: &mut CompactDef) {
        // Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
        // 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
        // This gives us 4 picks for 10 tables.
        // In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
        // then uses up a lot of memory for table builder.
        // We should keep it so we have at max 5 splits.
        let mut width = (cd.bot.len() as f64 / 5.0).ceil() as u32;
        if width < 3 {
            width = 3;
        }
        let mut skr = cd.this_range.clone();
        skr.extend(cd.next_range.clone());

        let mut add_range = |right: &Slice| {
            skr.right = right.clone();
            cd.splits.push(skr.clone());
            skr.left = skr.right.clone();
        };

        let tables = &self.levels[cd.next_level as usize].read().unwrap().tables;
        for (idx, table) in tables.iter().enumerate() {
            // last entry in bottom table
            if idx == tables.len() - 1 {
                add_range(table.max_key());
                return;
            }
            if idx as u32 % width == width - 1 {
                // set max key is right interval
                add_range(table.max_key())
            }
        }
    }

    pub async fn run_compacter(&self, id: u32) {
        // debug!
        println!("run compacter id : {}", id);

        // simulate random delay before starting the compaction process
        let random_delay = rand::thread_rng().gen_range(0..1000);
        sleep(Duration::from_millis(random_delay as u64)).await;

        // set up the periodic compaction ticker(every 5 seconds)
        let mut ticker = tokio::time::interval(Duration::from_millis(5000));
        loop {
            tokio::select! {
                // perform compaction once when the ticker triggers

                _= ticker.tick()=>{
                    if let Err(e) = self.run_once(id).await{
                        println!("{e}");
                    }
                }
            }
        }
    }

    async fn run_once(&self, id: u32) -> Result<(), String> {
        // debug
        println!("compact run once id : {}", id);
        let mut prios = self.pick_compact_levels()?;

        if id == 0 {
            // No.0 corountine, always tends to compress L0
            prios = Self::move_l0_to_front(prios);
        }
        for p in prios {
            if (id == 0 && p.level == 0) || p.adjusted >= 1.0 {
                return self.do_compact(id, p).await;
            }
        }
        Err("no compact".to_string())
    }

    async fn do_compact(&self, id: u32, p: CompactionPriority) -> Result<(), String> {
        let l = p.level;
        let base_level = p.t.base_level;
        // crate real compressing plan
        let mut cd = CompactDef {
            compact_id: id,
            t: p.t.clone(),
            this_level: p.level,
            p,
            next_level: 0,
            this_sz: 0,
            tables: Vec::new(),
            splits: Vec::new(),
            top: Vec::new(),
            bot: Vec::new(),
            this_range: KeyRange::new(),
            next_range: KeyRange::new(),
        };

        if l == 0 {
            cd.next_level = base_level;
            self.fill_tables_l0(&mut cd)?;
        } else {
            cd.next_level = cd.this_level;

            if cd.this_level != self.levels.len() as u32 {
                cd.next_level = cd.this_level + 1;
                self.fill_tables(&mut cd)?;
            }
        }

        self.run_compact_def(id, &mut cd).await?;

        Ok(())
    }

    async fn run_compact_def(&self, id: u32, cd: &mut CompactDef) -> Result<(), String> {
        //debug !
        println!("{:?}", cd);

        let this_level = cd.this_level;
        let next_level = cd.next_level;

        if this_level != next_level {
            self.add_splits(cd);
        }

        let new_tables = self.compact_build_tables(cd).await;

        let change_set = Self::build_change_set(cd, &new_tables);
        self.manifest_file
            .write()
            .unwrap()
            .add_changes(change_set.changes)?;

        let new_tables_id: Vec<u64> = new_tables.iter().map(|table| table.id().unwrap()).collect();

        self.replace_level_tables(cd.next_level, &cd.bot, new_tables);
        self.delete_level_tables(cd.this_level, &cd.bot);

        println!(
            "create new tables \n : {:?}\n delete tables :{:?}",
            new_tables_id, cd.tables
        );

        Ok(())
    }

    // compact_build_tables merge two level ssts
    async fn compact_build_tables(&self, cd: &CompactDef) -> Vec<Table> {
        // start parallel compression
        let (tx, mut rx) = mpsc::channel::<Table>(3);
        for kr in &cd.splits {
            let tx = tx.clone();
            let top = self.levels[cd.this_level as usize].clone();
            let bot = self.levels[cd.next_level as usize].clone();
            let cd = cd.clone();
            let kr = kr.clone();
            let opt = self.opt.clone();
            tokio::spawn(async move {
                Self::sub_compact(top, bot, cd, kr, tx, opt).await;
            });
        }
        drop(tx);

        let mut tables = Vec::new();
        while let Some(table) = rx.recv().await {
            tables.push(table);
        }
        tables.sort_by(|i, j| i.max_key().cmp(j.max_key()));
        tables
    }

    async fn sub_compact(
        top: Level,
        bot: Level,
        cd: CompactDef,
        kr: KeyRange,
        tx: mpsc::Sender<Table>,
        opt: Arc<Options>,
    ) {
        let top_tables = &top.read().unwrap().tables;
        let bot_tables = &bot.read().unwrap().tables;

        let mut v = Vec::new();
        for &i in &cd.top {
            v.push(top_tables[i as usize].new_iterator());
        }
        for &i in &cd.bot {
            v.push(bot_tables[i as usize].new_iterator());
        }

        let mut merge_iter = MergeIterator::new(v);
        let mut last_key = Slice::new();
        let mut add_keys = |iter: &mut MergeIterator, builder: &mut TableBuilder| -> bool {
            let mut table_kr = KeyRange::new();
            for (key, val) in iter {
                if key.cmp(&last_key) != Ordering::Equal {
                    if val.is_empty() {
                        last_key = key.clone();
                        continue;
                    }

                    // key range in iter greater or equal than tmp kr, break
                    if !kr.right.is_empty() && key.cmp(&kr.right).is_ge() {
                        return true;
                    }
                    if builder.reach_capacity() {
                        return false;
                    }

                    // set tmp key to last_key
                    last_key = key.clone();

                    // if left boundary is left, give tmp key to left boundary
                    if table_kr.left.is_empty() {
                        table_kr.left = key.clone();
                    }

                    // update right boundary
                    table_kr.right = last_key.clone();
                }
            }
            true
        };

        // if key range left live, seek to it
        if kr.left.is_empty() == false {
            merge_iter.seek(&kr.left);
        }

        loop {
            let mut table_builder = TableBuilder::new(opt.clone());
            let res = add_keys(&mut merge_iter, &mut table_builder);
            let opt = opt.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                Self::build_table(opt, table_builder, tx).await;
            });
            if res {
                return;
            }
        }
    }

    // a corountine to help build table
    async fn build_table(opt: Arc<Options>, table_builder: TableBuilder, tx: mpsc::Sender<Table>) {
        let new_id = opt
            .max_fid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let sst_name = file_helper::file_sstable_name(new_id);

        let table = Table::open(opt.clone(), sst_name, Some(table_builder)).unwrap();

        tx.send(table).await;
    }

    // build changeset
    fn build_change_set(cd: &CompactDef, new_tables: &Vec<Table>) -> ManifestChangeSet {
        let mut changes = Vec::new();
        for table in new_tables {
            changes.push(Self::new_create_change(table.id().unwrap(), cd.next_level));
        }
        for table in &cd.tables {
            changes.push(Self::new_delete_change(*table));
        }
        ManifestChangeSet { changes }
    }

    fn new_create_change(id: u64, level: u32) -> ManifestChange {
        ManifestChange {
            id,
            op: 0,
            level,
            checksum: Vec::new(),
        }
    }

    fn new_delete_change(id: u64) -> ManifestChange {
        ManifestChange {
            id,
            op: 1,
            level: 0,
            checksum: Vec::new(),
        }
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

    fn compact_state_overlap_with(&self, idx: usize, kr: &KeyRange) -> bool {
        let cs = self.compact_state.write().unwrap();
        cs.levels[idx].overlap_with(kr)
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
    pub fn new(opt: Arc<Options>) -> Self {
        let mut v = Vec::new();
        for _ in 0..opt.max_level_num as usize {
            v.push(LevelCompactStatus::default());
        }
        CompactStatus {
            levels: v,
            tables: HashSet::new(),
        }
    }
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
