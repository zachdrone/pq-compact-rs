use crate::plan::file_info::FileInfo;
use crate::plan::fingerprint::get_compaction_candidates_s3;
use crate::plan::get_compaction_candidates;
use arrow::record_batch::RecordBatch;
use clap::Parser;
use futures::{StreamExt, TryStreamExt, stream};
use num_cpus;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, arrow_writer::ArrowWriter};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::{
    io::BufWriter,
    path::{Path, PathBuf},
};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use std::sync::Arc;
use tokio::sync::mpsc;

pub mod plan;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    input: String,

    #[arg(short, long)]
    output: String,

    #[arg(long)]
    concurrency: Option<usize>,

    #[arg(long)]
    s3_limit: Option<usize>,

    #[arg(long)]
    cpu_limit: Option<usize>,
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

async fn compact_s3_files(
    files: Vec<FileInfo>,
    file_id: &str,
    out_dir_prefix: &object_store::path::Path,
    store: &Arc<dyn ObjectStore>,
) -> anyhow::Result<Vec<object_store::path::Path>> {
    let prefix = object_store::path::Path::from(files[0].path.clone());

    let parquet = ParquetObjectReader::new(store.clone(), prefix);

    let builder = ParquetRecordBatchStreamBuilder::new(parquet).await?;
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

    let make_writer = |idx: usize,
                       store: &Arc<dyn ObjectStore>|
     -> Result<
        (
            AsyncArrowWriter<ParquetObjectWriter>,
            object_store::path::Path,
        ),
        anyhow::Error,
    > {
        let path = out_dir_prefix.child(format!("{}_{}.parquet", file_id, idx));
        let object_store_writer = ParquetObjectWriter::new(store.clone(), path.clone());
        let writer = AsyncArrowWriter::try_new(
            object_store_writer,
            arrow_schema.clone(),
            Some(props.clone()),
        )?;
        Ok((writer, path))
    };

    let mut outputs = Vec::new();
    let mut file_idx = 0usize;
    let (mut writer, first_path) = make_writer(file_idx, &store)?;
    outputs.push(first_path);

    for info in files {
        let parquet =
            ParquetObjectReader::new(store.clone(), object_store::path::Path::from(info.path));
        let builder = ParquetRecordBatchStreamBuilder::new(parquet).await?;
        let mut stream = builder.build()?;

        while let Some(batch) = stream.next().await.transpose()? {
            if (writer.bytes_written() as u64) >= TARGET_FILE_BYTES {
                writer.close().await?;
                file_idx += 1;
                let (w, p) = make_writer(file_idx, &store)?;
                outputs.push(p);
                writer = w;
            }

            writer.write(&batch).await?;
        }
    }

    writer.close().await?;
    Ok(outputs)
}

#[derive(Clone)]
struct Limits {
    s3: Arc<Semaphore>,
    cpu: Arc<Semaphore>,
}

async fn compact_s3_files_v2(
    files: Vec<FileInfo>,
    file_id: &str,
    out_dir_prefix: &object_store::path::Path,
) -> anyhow::Result<Vec<object_store::path::Path>> {
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name("drone-sandbox")
            .build()
            .unwrap(),
    );

    let prefix = object_store::path::Path::from(files[0].path.clone());

    let parquet = ParquetObjectReader::new(store.clone(), prefix);

    let builder = ParquetRecordBatchStreamBuilder::new(parquet).await?;
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

    let make_writer = |idx: usize,
                       store: &Arc<dyn ObjectStore>|
     -> Result<
        (
            AsyncArrowWriter<ParquetObjectWriter>,
            object_store::path::Path,
        ),
        anyhow::Error,
    > {
        let path = out_dir_prefix.child(format!("{}_{}.parquet", file_id, idx));
        let object_store_writer = ParquetObjectWriter::new(store.clone(), path.clone());
        let writer = AsyncArrowWriter::try_new(
            object_store_writer,
            arrow_schema.clone(),
            Some(props.clone()),
        )?;
        Ok((writer, path))
    };

    let (tx, mut rx) = mpsc::channel::<RecordBatch>(32);

    let store_for_producers = store.clone();
    let producers: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        stream::iter(files.into_iter().map(|info| {
            let tx = tx.clone();
            let store = store_for_producers.clone();
            async move {
                let parquet =
                    ParquetObjectReader::new(store, object_store::path::Path::from(info.path));
                let builder = ParquetRecordBatchStreamBuilder::new(parquet).await?;
                let mut stream = builder.build()?;

                while let Some(batch) = stream.next().await.transpose()? {
                    if tx.send(batch).await.is_err() {
                        break;
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
        }))
        .buffer_unordered(16)
        .try_collect::<Vec<_>>()
        .await
        .map(|_| ())
    });

    let mut outputs = Vec::new();
    let mut file_idx = 0usize;
    let (mut writer, first_path) = make_writer(file_idx, &store)?;
    outputs.push(first_path);

    while let Some(b) = rx.recv().await {
        if (writer.bytes_written() as u64) >= TARGET_FILE_BYTES {
            writer.close().await?;
            file_idx += 1;
            let (w, p) = make_writer(file_idx, &store)?;
            outputs.push(p);
            writer = w;
        }

        writer.write(&b).await?;
    }
    writer.close().await?;

    producers.await??;

    Ok(outputs)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let outer = args.concurrency.unwrap_or(16);
    let s3_limit = args.s3_limit.unwrap_or(16);
    let cpu_limit = args.cpu_limit.unwrap_or(num_cpus::get());

    let limits = Limits {
        s3: Arc::new(Semaphore::new(s3_limit)),
        cpu: Arc::new(Semaphore::new(cpu_limit)),
    };

    let object_store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name("drone-sandbox")
            .build()
            .unwrap(),
    );

    let prefix = object_store::path::Path::from(format!("{}/", args.input));

    let candidates = get_compaction_candidates_s3(object_store.clone(), prefix.clone())
        .await
        .unwrap();

    let out_path = object_store::path::Path::from(args.output.clone());
    let _results = stream::iter(candidates.into_iter().map(|(fingerprint, files)| {
        let object_store = object_store.clone();
        let out_path = out_path.clone();
        let _limits = limits.clone();

        async move {
            let fp = fingerprint;
            match compact_s3_files_v2(files, &fp, &out_path).await {
                Ok(outputs) => Ok::<Vec<object_store::path::Path>, anyhow::Error>(outputs),
                Err(e) => Err(anyhow::anyhow!("{fp} failed: {e}")),
            }
        }
    }))
    .buffer_unordered(outer)
    .collect::<Vec<_>>()
    .await;

    // let candidates = get_compaction_candidates(&args.input).unwrap();
    //
    // for (fingerprint, files) in candidates {
    //     let _ = match compact_local_files(files, &fingerprint, Path::new("output")).await {
    //         Ok(_) => continue,
    //         Err(_) => println!("something failed"),
    //     };
    // }
}
