use anyhow::Result;
use glob::glob;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaDataReader;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use tokio::fs;

#[derive(Debug)]
struct FileInfo {
    path: String,
    file_size: u64,
    avg_row_group_size: i64,
}

impl FileInfo {
    fn is_candidate(&self) -> bool {
        if self.file_size < 64 * 1024 || self.avg_row_group_size < 32 * 1024 {
            return true;
        }
        return false;
    }
}

async fn get_file_sizes(directory: &str) -> Result<Vec<FileInfo>, anyhow::Error> {
    let mut handles = Vec::new();
    let mut results = Vec::new();
    for entry in glob(&format!("{}/*.parquet", directory)).expect("Failed to read glob pattern") {
        if let Ok(fp) = entry {
            handles.push(tokio::spawn(async move {
                let file_size = tokio::fs::metadata(&fp).await.unwrap().size();
                let file = File::open(&fp).unwrap();
                let md = ParquetMetaDataReader::new()
                    .with_page_indexes(true)
                    .parse_and_finish(&file)
                    .unwrap();
                let total_size: i64 = md.row_groups().iter().map(|rg| rg.total_byte_size()).sum();
                let row_group_count = md.num_row_groups();

                FileInfo {
                    path: fp.to_str().unwrap().to_string(),
                    file_size: file_size,
                    avg_row_group_size: total_size / (row_group_count as i64),
                }
            }))
        }
    }

    for handle in handles {
        results.push(handle.await.unwrap());
    }

    Ok(results)
}

#[tokio::main]
async fn main() {
    let results = get_file_sizes("files").await.unwrap();

    for result in results {
        println!("{:?}", result);
    }
}
