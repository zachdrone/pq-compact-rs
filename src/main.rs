use anyhow::Result;
use blake3::Hasher;
use glob::glob;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::types::{BasicTypeInfo, SchemaDescriptor};
use parquet::{
    basic::LogicalType,
    basic::{ConvertedType, Repetition, TimeUnit, Type as PhysicalType},
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
                let schema = md.file_metadata().schema_descr();

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

fn build_node(t: &Type) -> CanonNode {
    let basic = t.get_basic_info();
    let name = basic.name();
    let rep = if basic.has_repetition() {
        Some(basic.repetition().to_string())
    } else {
        None
    };
    let id = if basic.has_id() {
        Some(basic.id())
    } else {
        None
    };
    let logical = effective_logical(t);
}

fn rep_to_str(r: Repetition) -> &'static str {
    match r {
        Repetition::REQUIRED => "required",
        Repetition::OPTIONAL => "optional",
        Repetition::REPEATED => "repeated",
    }
}

fn physical_to_str(p: PhysicalType) -> &'static str {
    match p {
        PhysicalType::BOOLEAN => "boolean",
        PhysicalType::INT32 => "int32",
        PhysicalType::INT64 => "int64",
        PhysicalType::INT96 => "int96",
        PhysicalType::FLOAT => "float",
        PhysicalType::DOUBLE => "double",
        PhysicalType::BYTE_ARRAY => "byte_array",
        PhysicalType::FIXED_LEN_BYTE_ARRAY => "fixed_len_byte_array",
    }
}

fn unit_to_str(u: TimeUnit) -> &'static str {
    match u {
        TimeUnit::MILLIS(_) => "millis",
        TimeUnit::MICROS(_) => "micros",
        TimeUnit::NANOS(_) => "nanos",
    }
}

fn effective_logical(t: &Type) -> Option<CanonLogical> {
    let basic = t.get_basic_info();

    if let Some(lt) = basic.logical_type() {
        return Some(match lt {
            LogicalType::String => CanonLogical::String,
            LogicalType::Map => CanonLogical::Map,
            LogicalType::List => CanonLogical::List,
            LogicalType::Enum => CanonLogical::Enum,
            LogicalType::Decimal { scale, precision } => CanonLogical::Decimal { precision, scale },
            LogicalType::Date => CanonLogical::Date,
            LogicalType::Time {
                is_adjusted_to_u_t_c,
                unit,
            } => CanonLogical::Time {
                unit: unit_to_str(unit).to_string(),
                utc: is_adjusted_to_u_t_c,
            },
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            } => CanonLogical::Timestamp {
                unit: unit_to_str(unit).to_string(),
                utc: is_adjusted_to_u_t_c,
            },
            LogicalType::Integer {
                bit_width,
                is_signed,
            } => CanonLogical::Integer {
                bit_width: bit_width,
                signed: is_signed,
            },
            LogicalType::Unknown => CanonLogical::Unknown,
            LogicalType::Json => CanonLogical::Json,
            LogicalType::Bson => CanonLogical::Bson,
            LogicalType::Uuid => CanonLogical::Uuid,
            LogicalType::Float16 => CanonLogical::Float16,
            LogicalType::Variant => CanonLogical::Variant,
            LogicalType::Geometry => CanonLogical::Geometry,
            LogicalType::Geography => CanonLogical::Geography,
        });
    } else {
        None
    }
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
