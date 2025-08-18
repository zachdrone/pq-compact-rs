use crate::plan::file_info::FileInfo;
use parquet::{
    basic::LogicalType,
    basic::{TimeUnit, Type as PhysicalType},
    schema::types::Type,
};
use serde::Serialize;

use anyhow::Result;
use glob::glob;
use parquet::file::metadata::ParquetMetaDataReader;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;

fn build_node(t: &Type) -> CanonNode {
    let basic = t.get_basic_info();
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

    if t.is_group() {
        let children = t
            .get_fields()
            .iter()
            .map(|child| build_node(child.as_ref()))
            .collect::<Vec<_>>();
        CanonNode {
            name: basic.name().to_string(),
            rep,
            logical,
            physical: None,
            length: None,
            precision: None,
            scale: None,
            id,
            children: Some(children),
        }
    } else {
        let physical = Some(physical_to_str(t.get_physical_type()).to_string());

        let length = match t {
            Type::PrimitiveType {
                physical_type: PhysicalType::FIXED_LEN_BYTE_ARRAY,
                type_length,
                ..
            } if *type_length > 0 => Some(*type_length),
            _ => None,
        };

        let (precision, scale) = match logical {
            Some(CanonLogical::Decimal { .. }) => match t {
                Type::PrimitiveType {
                    precision, scale, ..
                } => (Some(*precision), Some(*scale)),
                _ => (None, None),
            },
            _ => (None, None),
        };

        CanonNode {
            name: basic.name().to_string(),
            rep,
            logical,
            physical,
            length,
            precision,
            scale,
            id,
            children: None,
        }
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

#[derive(Debug, Serialize)]
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
    length: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    precision: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    scale: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<Vec<CanonNode>>,
}

#[derive(Debug, Serialize)]
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

fn is_candidate(file_size: &u64, avg_row_group_size: &i64) -> bool {
    if *file_size < 64 * 1024 * 1024 || *avg_row_group_size < 32 * 1024 * 1024 {
        return true;
    }
    return false;
}

pub fn get_compaction_candidates(dir: &str) -> Result<HashMap<String, Vec<FileInfo>>> {
    let mut results = HashMap::new();

    for entry in glob(&format!("{dir}/*.parquet")).expect("Failed to read glob pattern") {
        let fp = entry?;

        let file_size = std::fs::metadata(&fp)?.len();

        let file = File::open(&fp)?;

        let md = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_and_finish(&file)?;

        let row_group_count = md.num_row_groups();
        if row_group_count == 0 {
            anyhow::bail!("File {:?} has no row groups", fp)
        }

        let total_size: i64 = md.row_groups().iter().map(|rg| rg.total_byte_size()).sum();

        let avg_row_group_size = total_size / (row_group_count as i64);

        if is_candidate(&file_size, &avg_row_group_size) {
            let schema = md.file_metadata().schema_descr();
            let node = build_node(schema.root_schema());
            let canonical = serde_json::to_string(&node)?;

            let mut hasher = Sha256::new();
            hasher.update(canonical.as_bytes());
            let digest = hasher.finalize();
            let fingerprint = hex::encode(digest);

            let value = FileInfo {
                path: fp.to_str().unwrap().to_string(),
                file_size: file_size,
                avg_row_group_size: total_size / (row_group_count as i64),
            };
            results
                .entry(fingerprint)
                .or_insert_with(Vec::new)
                .push(value);
        }
    }

    Ok(results)
}
