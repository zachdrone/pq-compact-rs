use crate::plan::get_compaction_candidates;
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

        let file = File::create(format!("output/out_{}.parquet", i)).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        let mut bytes_written = 0;
        for file_info in files {
            let file = File::open(&file_info.path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mut reader: ParquetRecordBatchReader = builder.build().unwrap();
            println!("reading file {}", file_info.path);
            while let Some(batch) = reader.next() {
                let batch = batch.unwrap();
                let _ = writer.write(&batch);
                bytes_written += (batch.num_rows() as i64) * file_info.avg_row_comp_bytes;
                if bytes_written >= 128 * 1024 * 1024 {
                    println!("flushing {} bytes", bytes_written);
                    writer.flush().unwrap();
                    bytes_written = 0;
                }
            }
            writer.flush().unwrap()
        }
        writer.close().unwrap();
        i += 1;
    }
}
