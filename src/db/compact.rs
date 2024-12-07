struct CompactionPriority {
    level: u32,
    score: f64,
    adjusted: f64,
    target: Target,
}

struct Target {
    base_level: u32,
}

struct CompactDef{
    compact_id : u32,
}