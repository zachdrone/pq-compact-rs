use crate::plan::get_compaction_candidates;
use clap::Parser;

pub mod plan;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    dir: String,
}

fn main() {
    let args = Args::parse();

    let results = get_compaction_candidates(&args.dir).unwrap();

    for result in results {
        println!("{:?}", result);
    }
}
