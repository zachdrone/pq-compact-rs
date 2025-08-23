use crate::plan::get_compaction_candidates;
use arrow::record_batch::RecordBatch;
use arrow_select::concat::concat_batches;
use clap::Parser;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;
use parquet::file::serialized_reader;
use parquet::{
    arrow::{
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
        arrow_writer::{ArrowWriter, ArrowWriterOptions},
    },
    file::reader::{FileReader, SerializedFileReader},
};
use std::fs::File;
use std::io::{self, Write};

pub mod plan;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    dir: String,
}

fn main() {
    let args = Args::parse();

    let candidates = get_compaction_candidates(&args.dir).unwrap();

    let mut i = 0;
    for (_, files) in candidates {
        let file = File::open(&files[0].path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let arrow_schema = builder.schema().clone();

        let avg_row_size: i64 = files
            .iter()
            .map(|info| info.avg_row_comp_bytes)
            .sum::<i64>()
            / (files.len() as i64);
        let target_rg_size: i64 = 128 * 1024 * 1024;
        let max_rg_size = (target_rg_size / avg_row_size) as usize;

        let mut file_num = 0;
        let file = File::create(format!("output/out_{}_{}.parquet", i, file_num)).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(max_rg_size)
            .build();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();

        for file_info in files {
            let file = File::open(&file_info.path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mut reader: ParquetRecordBatchReader = builder.build().unwrap();
            while let Some(batch) = reader.next() {
                let batch = batch.unwrap();
                let _ = writer.write(&batch);
                writer.flush().unwrap();
                if writer.bytes_written() >= 512 * 1024 * 1024 {
                    file_num += 1;
                    let file =
                        File::create(format!("output/out_{}_{}.parquet", i, file_num)).unwrap();
                    let props = WriterProperties::builder()
                        .set_max_row_group_size(max_rg_size)
                        .build();
                    writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
                }
            }
        }
        writer.close().unwrap();
        i += 1;
    }
}
