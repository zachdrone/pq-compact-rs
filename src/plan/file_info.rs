#[derive(Debug)]
pub struct FileInfo {
    pub path: String,
    pub file_size: u64,
    pub avg_rg_comp_bytes: i64,
    pub avg_row_comp_bytes: i64,
}
