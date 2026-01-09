pub fn make_item_id(plan_hash: &str, table: &str, idx: usize) -> String {
    // Stable & human-ish: plan-hash + item-index + dest-name
    let mut h = blake3::Hasher::new();
    h.update(plan_hash.as_bytes());
    h.update(b":");
    h.update(idx.to_string().as_bytes());
    h.update(b":");
    h.update(table.as_bytes());
    format!("itm-{}", &h.finalize().to_hex()[..16])
}
