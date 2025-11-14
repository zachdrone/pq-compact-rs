use crate::plan::file_info::FileInfo;
use crate::{MAX_RG_ROWS, MIN_RG_ROWS, TARGET_FILE_BYTES, TARGET_RG_BYTES};

use arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt, stream};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tokio::task::JoinHandle;

use object_store::ObjectStore;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use std::sync::Arc;
use tokio::sync::mpsc;

pub async fn compact_s3_files(
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

    let store = store.clone();
    let props = props.clone();
    let out_dir_prefix = out_dir_prefix.clone();
    let file_id = file_id.to_string();

    let make_writer = move |idx: usize,
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

    let mut o = Vec::new();
    let mut file_idx = 0usize;
    let (mut writer, first_path) = make_writer(file_idx, &store)?;
    o.push(first_path);

    let outputs = tokio::spawn(async move {
        while let Some(b) = rx.recv().await {
            if (writer.bytes_written() as u64) >= TARGET_FILE_BYTES {
                writer.close().await?;
                file_idx += 1;
                let (w, p) = make_writer(file_idx, &store)?;
                o.push(p);
                writer = w;
            }

            writer.write(&b).await?;
        }
        writer.close().await?;

        producers.await??;
        Ok::<Vec<object_store::path::Path>, anyhow::Error>(o)
    });

    Ok(outputs.await??)
}
