use crate::plan::fingerprint::get_compaction_candidates;

pub mod plan;

#[tokio::main]
async fn main() {
    let results = get_compaction_candidates("files").unwrap();

    for result in results {
        println!("{:?}", result);
    }
}
