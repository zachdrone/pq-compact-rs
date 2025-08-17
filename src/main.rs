use anyhow::Result;
use blake3::Hasher;
use glob::glob;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::types::{BasicTypeInfo, SchemaDescriptor};
use parquet::{
    basic::LogicalType,
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    schema::{parser, printer, types::Type},
};
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::MetadataExt;

#[derive(Debug)]
struct FileInfo {
    path: String,
    file_size: u64,
    avg_row_group_size: i64,
}

impl FileInfo {
    fn is_candidate(&self) -> bool {
        if self.file_size < 64 * 1024 * 1024 || self.avg_row_group_size < 32 * 1024 * 1024 {
            return true;
        }
        return false;
    }
}

async fn get_compaction_candidates(directory: &str) -> Result<Vec<FileInfo>, anyhow::Error> {
    let mut handles: Vec<tokio::task::JoinHandle<Result<FileInfo>>> = Vec::new();
    let mut results = Vec::new();
    for entry in
        glob(&format!("{}/nested*.parquet", directory)).expect("Failed to read glob pattern")
    {
        if let Ok(fp) = entry {
            handles.push(tokio::spawn(async move {
                let file_size = tokio::fs::metadata(&fp).await?.size();
                let file = File::open(&fp)?;
                let md = ParquetMetaDataReader::new()
                    .with_page_indexes(true)
                    .parse_and_finish(&file)?;
                let total_size: i64 = md.row_groups().iter().map(|rg| rg.total_byte_size()).sum();
                let row_group_count = md.num_row_groups();
                dbg!(&md.file_metadata().schema_descr().root_schema());
                fingerprint_schema_exact(&md.file_metadata().schema_descr());

                Ok(FileInfo {
                    path: fp.to_str().unwrap().to_string(),
                    file_size: file_size,
                    avg_row_group_size: total_size / (row_group_count as i64),
                })
            }))
        }
    }

    for handle in handles {
        if let Ok(file_info) = handle.await? {
            if file_info.is_candidate() {
                results.push(file_info)
            }
        }
    }

    Ok(results)
}

// async fn plan_file_compaction(candidates: &Vec<FileInfo>) {}

fn emit_type(h: &mut Hasher, t: &Type) {
    let mut line = String::new();

    let info: parquet::schema::types::BasicTypeInfo = t.get_basic_info().clone();
    let name = info.name();
    let rep = if info.has_repetition() {
        match info.repetition() {
            Repetition::REQUIRED => "REQUIRED",
            Repetition::OPTIONAL => "OPTIONAL",
            Repetition::REPEATED => "REPEATED",
        }
    } else {
        ""
    };
    let field_id = if info.has_id() {
        info.id().to_string()
    } else {
        String::new()
    };

    let logical = match info.logical_type() {
        Some(v) => match v.try_into(String) {
            Ok(val) => val,
            Err(_) => "",
        },
        None => "",
    };
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct CanonNode {
    name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    rep: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    logical: Option<CanonLogical>,

    #[serde(skip_serializing_if = "Option::is_none")]
    physical: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    precision: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    scale: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<Vec<CanonNode>>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanonLogical {
    String,
    Map,
    List,
    Enum,
    Decimal { precision: i32, scale: i32 },
    Date,
    Time { unit: String, utc: bool },
    Timestamp { unit: String, utc: bool },
    Integer { bit_width: i8, signed: bool },
    Unknown,
    Json,
    Bson,
    Uuid,
    Float16,
    Variant,
    Geometry,
    Geography,
}

#[tokio::main]
async fn main() {
    let results = get_compaction_candidates("files").await.unwrap();

    for result in results {
        println!("{:?}", result);
    }
}
