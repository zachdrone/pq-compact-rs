#[derive(Debug)]
pub struct FileInfo {
    pub path: String,
    pub file_size: u64,
    pub avg_row_group_size: i64,
}
