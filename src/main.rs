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

        let file = File::create(format!("out_{}.parquet", i)).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(512)
            .build();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        for file_info in files {
            let file = File::open(&file_info.path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mut reader: ParquetRecordBatchReader =
                builder.with_batch_size(512).build().unwrap();
            println!("reading file {}", file_info.path);
            while let Some(batch) = reader.next() {
                let batch = batch.unwrap();
                let _ = writer.write(&batch);
                dbg!(writer.bytes_written());
                writer.flush().unwrap();
                dbg!(writer.bytes_written()); // Number of bytes written to sink (after flush)

                // let size_bytes: usize = batch
                //     .unwrap()
                //     .columns()
                //     .iter()
                //     .map(|array| array.get_array_memory_size())
                //     .sum();
                // println!("{:?}", size_bytes);
                // dbg!(file_info.avg_row_group_size / (batch.num_rows() as i64));
            }
        }
        writer.close().unwrap();
        i += 1;
    }
}
