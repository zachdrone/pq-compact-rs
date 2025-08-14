use anyhow::Result;
use glob::glob;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaDataReader;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use tokio::fs;

async fn get_file_sizes(directory: &str) -> Result<Vec<u64>, anyhow::Error> {
    let mut handles = Vec::new();
    let mut results = Vec::new();
    for entry in glob(&format!("{}/*.parquet", directory)).expect("Failed to read glob pattern") {
        if let Ok(fp) = entry {
            handles.push(tokio::spawn(async move {
                let f = tokio::fs::metadata(fp).await.unwrap();
                f.size()
            }))
        }
    }

    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // for result in results {
    //     println!("{:?}", result);
    // }
    Ok(results)
}

#[tokio::main]
async fn main() {
    let results = get_file_sizes("files").await.unwrap();

    for result in results {
        println!("{:?}", result);
    }

    // let f = fs::metadata("files/file_01.parquet")
    //     .await
    //     .expect("Failed to read metadata");
    // let size = f.size();
    //
    //
    // print!("{:?}", size.size());

    // let file = File::open("file_01.parquet").unwrap();
    // let md = ParquetMetaDataReader::new()
    //     .with_page_indexes(true)
    //     .parse_and_finish(&file)
    //     .unwrap();
    // // println!("{}", md.row_groups()[0].total_byte_size());
    // // println!("{:?}", md.row_groups()[0]);
    // println!("{:?}", md.file_metadata());
    // let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    // println!("Converted arrow schema is: {}", builder.schema());
    //
    // let mut reader = builder.build().unwrap();
    //
    //
    // let record_batch = reader.next().unwrap().unwrap();
    //
    // println!("Read {} records.", record_batch.num_rows())
}
