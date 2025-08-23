use crate::plan::file_info::FileInfo;
use crate::plan::get_compaction_candidates;
use clap::Parser;
use parquet::arrow::{
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    arrow_writer::ArrowWriter,
};
use parquet::file::properties::WriterProperties;
use std::fs::File;

pub mod plan;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    dir: String,
}

fn compact_files(files: Vec<FileInfo>, file_id: &str) -> anyhow::Result<()> {
    // Get schema
    let file = File::open(&files[0].path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema().clone();

    // Compute max rg size
    let avg_row_size: i64 = files
        .iter()
        .map(|info| info.avg_row_comp_bytes)
        .sum::<i64>()
        / (files.len() as i64);
    let target_rg_size: i64 = 128 * 1024 * 1024;
    let max_rg_size = (target_rg_size / avg_row_size) as usize;

    // Create first output file and writer
    let mut file_num = 0;
    let file = File::create(format!("output/out_{}_{}.parquet", file_id, file_num))?;
    let props = WriterProperties::builder()
        .set_max_row_group_size(max_rg_size)
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;

    // Loop through each file
    for file_info in files {
        // Read file
        let file = File::open(&file_info.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader: ParquetRecordBatchReader = builder.build()?;
        while let Some(batch) = reader.next() {
            let batch = batch?;
            let _ = writer.write(&batch);
            writer.flush()?;

            // Write to new file if upper bound for file size has been surpassed
            if writer.bytes_written() >= 512 * 1024 * 1024 {
                writer.close()?;
                file_num += 1;
                let file = File::create(format!("output/out_{}_{}.parquet", file_id, file_num))?;
                let props = WriterProperties::builder()
                    .set_max_row_group_size(max_rg_size)
                    .build();
                writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;
            }
        }
    }
    writer.close()?;
    Ok(())
}

fn main() {
    let args = Args::parse();

    let candidates = get_compaction_candidates(&args.dir).unwrap();

    for (fingerprint, files) in candidates {
        let _ = match compact_files(files, &fingerprint) {
            Ok(_) => continue,
            Err(_) => println!("something failed"),
        };
    }
}
