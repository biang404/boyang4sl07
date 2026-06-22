use rustc_hash::FxHashMap;

pub fn reduce_entries(entries: Vec<(String, u32)>) -> FxHashMap<String, u32> {
    let mut out: FxHashMap<String, u32> = FxHashMap::default();
    for (k, v) in entries {
        let count = out.entry(k).or_insert(0);
        *count += v;
    }
    out
}

pub fn map_to_sorted_vec(map: FxHashMap<String, u32>) -> Vec<(String, u32)> {
    let mut entries: Vec<(String, u32)> = map.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}
