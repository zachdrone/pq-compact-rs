use crate::plan::file_info::FileInfo;
use crate::plan::fingerprint::get_compaction_candidates_async;
use crate::plan::get_compaction_candidates;
use clap::Parser;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, arrow_writer::ArrowWriter};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::{
    io::BufWriter,
    path::{Path, PathBuf},
};

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
// use object_store::path::Path as pqPath;
use parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use std::sync::Arc;

pub mod plan;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    dir: String,
}

const TARGET_RG_BYTES: u64 = 56 * 1024 * 1024;
const TARGET_FILE_BYTES: u64 = 128 * 1024 * 1024;
const MIN_RG_ROWS: usize = 100;
const MAX_RG_ROWS: usize = 4_000_000;

async fn compact_local_files(
    files: Vec<FileInfo>,
    file_id: &str,
    out_dir: &Path,
) -> anyhow::Result<Vec<PathBuf>> {
    let file = File::open(&files[0].path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema().clone();

    let avg_row_size_bytes: u64 = {
        let sum: i128 = files.iter().map(|f| f.avg_row_comp_bytes as i128).sum();
        let n = files.len() as i128;
        let avg = if n > 0 { sum / n } else { 0 };
        if avg <= 0 { 1024 } else { avg as u64 }
    };
    let max_rg_rows = (TARGET_RG_BYTES / avg_row_size_bytes)
        .clamp(MIN_RG_ROWS as u64, MAX_RG_ROWS as u64) as usize;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(true)
        .set_max_row_group_size(max_rg_rows)
        .build();

    let make_writer =
        |idx: usize| -> Result<(ArrowWriter<BufWriter<File>>, PathBuf), anyhow::Error> {
            let path = out_dir.join(format!("{}_{}.parquet", file_id, idx));
            let file = File::create(&path)?;
            let buf = BufWriter::new(file);
            let writer = ArrowWriter::try_new(buf, arrow_schema.clone(), Some(props.clone()))?;
            Ok((writer, path))
        };

    let mut outputs = Vec::new();
    let mut file_idx = 0usize;
    let (mut writer, first_path) = make_writer(file_idx)?;
    outputs.push(first_path);

    for info in files {
        let f = File::open(&info.path)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(f)?.build()?;

        while let Some(batch) = reader.next() {
            let batch = batch?;

            if (writer.bytes_written() as u64) >= TARGET_FILE_BYTES {
                writer.close()?;
                file_idx += 1;
                let (w, p) = make_writer(file_idx)?;
                outputs.push(p);
                writer = w;
            }

            writer.write(&batch)?;
        }
    }

    writer.close()?;
    Ok(outputs)
}

// async fn compact_s3_files(
//     files: Vec<FileInfo>,
//     file_id: &str,
//     out_dir: &Path,
// ) -> anyhow::Result<Vec<PathBuf>> {
//     let object_store: Arc<dyn ObjectStore> = Arc::new(
//         AmazonS3Builder::from_env()
//             .with_bucket_name("drone-sandbox")
//             .build()
//             .unwrap(),
//     );
//
//     let prefix = object_store::path::Path::from("files/file_0.parquet");
//
//     let parquet = ParquetObjectReader::new(object_store, prefix);
//
//     let builder = ParquetRecordBatchStreamBuilder::new(parquet).await.unwrap();
//     let arrow_schema = builder.schema().clone();
//
//     let avg_row_size_bytes: u64 = {
//         let sum: i128 = files.iter().map(|f| f.avg_row_comp_bytes as i128).sum();
//         let n = files.len() as i128;
//         let avg = if n > 0 { sum / n } else { 0 };
//         if avg <= 0 { 1024 } else { avg as u64 }
//     };
//     let max_rg_rows = (TARGET_RG_BYTES / avg_row_size_bytes)
//         .clamp(MIN_RG_ROWS as u64, MAX_RG_ROWS as u64) as usize;
//
//     let props = WriterProperties::builder()
//         .set_compression(Compression::ZSTD(ZstdLevel::default()))
//         .set_dictionary_enabled(true)
//         .set_max_row_group_size(max_rg_rows)
//         .build();
//
//     let make_writer =
//         |idx: usize| -> Result<(ArrowWriter<BufWriter<File>>, PathBuf), anyhow::Error> {
//             let path = out_dir.join(format!("{}_{}.parquet", file_id, idx));
//             let file = File::create(&path)?;
//             let buf = BufWriter::new(file);
//             let writer = ArrowWriter::try_new(buf, arrow_schema.clone(), Some(props.clone()))?;
//             Ok((writer, path))
//         };
//
//     let mut outputs = Vec::new();
//     let mut file_idx = 0usize;
//     let (mut writer, first_path) = make_writer(file_idx)?;
//     outputs.push(first_path);
//
//     for info in files {
//         let f = File::open(&info.path)?;
//         let mut reader = ParquetRecordBatchReaderBuilder::try_new(f)?.build()?;
//
//         let mut stream = builder.build().unwrap();
//
//         while let Some(batch) = reader.next() {
//             let batch = batch?;
//
//             if (writer.bytes_written() as u64) >= TARGET_FILE_BYTES {
//                 writer.close()?;
//                 file_idx += 1;
//                 let (w, p) = make_writer(file_idx)?;
//                 outputs.push(p);
//                 writer = w;
//             }
//
//             writer.write(&batch)?;
//         }
//     }
//
//     writer.close()?;
//     Ok(outputs)
// }

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let object_store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name("drone-sandbox")
            .build()
            .unwrap(),
    );

    let prefix = object_store::path::Path::from("files/");

    let candidates = get_compaction_candidates_async(object_store, prefix).await;

    // let candidates = get_compaction_candidates(&args.dir).unwrap();

    // for (fingerprint, files) in candidates {
    //     let _ = match compact_local_files(files, &fingerprint, Path::new("output")).await {
    //         Ok(_) => continue,
    //         Err(_) => println!("something failed"),
    //     };
    // }
}
