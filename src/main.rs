use crate::plan::get_compaction_candidates;
use clap::Parser;
use parquet::arrow::{
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    arrow_writer::{ArrowWriter, ArrowWriterOptions},
};
use parquet::file::properties::WriterProperties
use parquet::file::serialized_reader;
use std::fs::File;

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
        let mut total = 0;
        let mut curr_row = 0;
        let file = File::create(format!("out_{}.parquet",i)).unwrap();
        let props = WriterProperties::builder().set_max_row_group_size(512).build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        for file_info in files {
            let file = File::open(&file_info.path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mut reader: ParquetRecordBatchReader =
                builder.with_batch_size(512).build().unwrap();
            while let Some(batch) = reader.next() {
                let batch = batch.unwrap();
                // let size_bytes: usize = batch
                //     .unwrap()
                //     .columns()
                //     .iter()
                //     .map(|array| array.get_array_memory_size())
                //     .sum();
                // println!("{:?}", size_bytes);
                // dbg!(file_info.avg_row_group_size / (batch.num_rows() as i64));
            }
            break;
        }
    }
}
