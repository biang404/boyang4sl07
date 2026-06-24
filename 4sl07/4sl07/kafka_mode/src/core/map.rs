use rustc_hash::FxHashMap;
use std::io::Read;

pub fn map_chunk_from_file(
    path: &str,
    offset: u64,
    chunk_size_bytes: usize,
) -> std::io::Result<FxHashMap<String, u32>> {
    let mut file = std::fs::File::open(path)?;
    use std::io::Seek;
    use std::io::SeekFrom;
    file.seek(SeekFrom::Start(offset))?;

    let mut buf = vec![0_u8; chunk_size_bytes];
    let read_len = file.read(&mut buf)?;
    buf.truncate(read_len);

    let mut text = String::from_utf8_lossy(&buf).to_string();
    text.make_ascii_lowercase();

    let mut out: FxHashMap<String, u32> = FxHashMap::default();
    for word in text.split(|c: char| {
        c == ' '
            || c == '\n'
            || c == '\r'
            || c == '.'
            || c == ','
            || c == '?'
            || c == ':'
            || c == '!'
            || c == '('
            || c == ')'
            || c == ';'
            || c == '-'
            || c == '_'
            || c == '"'
            || c == '{'
            || c == '}'
            || c == '['
            || c == ']'
            || c == '+'
            || c == '='
            || c == '/'
            || c == '\\'
    }) {
        if word.is_empty() {
            continue;
        }
        let count = out.entry(word.to_string()).or_insert(0);
        *count += 1;
    }

    // Explicitly free large temporary buffers as soon as map task is done.
    buf.clear();
    buf.shrink_to_fit();
    text.clear();
    text.shrink_to_fit();

    Ok(out)
}

pub fn partition_map(map: FxHashMap<String, u32>, reduce_count: usize) -> Vec<Vec<(String, u32)>> {
    let mut partitions: Vec<Vec<(String, u32)>> = (0..reduce_count).map(|_| Vec::new()).collect();
    for (k, v) in map {
        let idx = (fxhash32(k.as_bytes()) as usize) % reduce_count;
        partitions[idx].push((k, v));
    }
    partitions
}

fn fxhash32(data: &[u8]) -> u32 {
    use std::hash::Hasher;
    let mut hasher = rustc_hash::FxHasher::default();
    hasher.write(data);
    hasher.finish() as u32
}
