use crate::compact::remote::compact_s3_files;
use crate::plan::fingerprint::get_compaction_candidates_s3;
use clap::Parser;
use futures::{StreamExt, stream};
use num_cpus;
use tokio::sync::Semaphore;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

pub mod compact;
pub mod plan;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    input: String,

    #[arg(short, long)]
    output: String,

    #[arg(long)]
    input_bucket: String,

    #[arg(long)]
    output_bucket: String,

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

#[derive(Clone)]
struct Limits {
    s3: Arc<Semaphore>,
    cpu: Arc<Semaphore>,
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

    let input_store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(args.input_bucket)
            .build()
            .unwrap(),
    );

    let output_store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(args.output_bucket)
            .build()
            .unwrap(),
    );

    let prefix = object_store::path::Path::from(format!("{}/", args.input));

    let candidates = get_compaction_candidates_s3(input_store.clone(), prefix.clone())
        .await
        .unwrap();

    let out_path = object_store::path::Path::from(args.output.clone());
    let _results = stream::iter(candidates.into_iter().map(|(fingerprint, files)| {
        let store = output_store.clone();
        let out_path = out_path.clone();
        let _limits = limits.clone();

        async move {
            let fp = fingerprint;
            match compact_s3_files(files, &fp, &out_path, &store).await {
                Ok(outputs) => Ok::<Vec<object_store::path::Path>, anyhow::Error>(outputs),
                Err(e) => Err(anyhow::anyhow!("{fp} failed: {e}")),
            }
        }
    }))
    .buffer_unordered(outer)
    .collect::<Vec<_>>()
    .await;
}
