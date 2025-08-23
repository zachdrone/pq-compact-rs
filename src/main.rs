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

fn compact_files(files: Vec<FileInfo>, file_id: &str) {
    // Get schema
    let file = File::open(&files[0].path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
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
    let file = File::create(format!("output/out_{}_{}.parquet", file_id, file_num)).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(max_rg_size)
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();

    // Loop through each file
    for file_info in files {
        // Read file
        let file = File::open(&file_info.path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader: ParquetRecordBatchReader = builder.build().unwrap();
        while let Some(batch) = reader.next() {
            let batch = batch.unwrap();
            let _ = writer.write(&batch);
            writer.flush().unwrap();

            // Write to new file if upper bound for file size has been surpassed
            if writer.bytes_written() >= 512 * 1024 * 1024 {
                writer.close().unwrap();
                file_num += 1;
                let file =
                    File::create(format!("output/out_{}_{}.parquet", file_id, file_num)).unwrap();
                let props = WriterProperties::builder()
                    .set_max_row_group_size(max_rg_size)
                    .build();
                writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
            }
        }
    }
    writer.close().unwrap();
}

fn main() {
    let args = Args::parse();

    let candidates = get_compaction_candidates(&args.dir).unwrap();

    for (fingerprint, files) in candidates {
        compact_files(files, &fingerprint);
    }
}
