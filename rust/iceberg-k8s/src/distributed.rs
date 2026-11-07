//! Distributed trip statistics over an Iceberg table, in three steps (plan, map, reduce) so the
//! scan can be split across the pods of a Kubernetes Indexed Job:
//!
//! 1. `plan_trip_stats` pins the table's current snapshot, plans the scan once and assigns the
//!    data files to `shard_count` shards, balanced by size. It writes `<run_uri>/plan.json`
//!    (only if it does not exist yet, so two runs cannot share a run URI).
//! 2. `trip_stats_shard` runs once per shard (the index comes from `JOB_COMPLETION_INDEX` in an
//!    Indexed Job). It plans the scan of the same snapshot again, keeps the files of its shard,
//!    reads only the statistics columns and writes `<run_uri>/shards/shard-<index>.json`.
//! 3. `merge_trip_stats` checks that every shard read exactly the files the plan gave it, merges
//!    the partial results into the final `trip_stats` and writes `<run_uri>/result.json`.
//!
//! The plan holds file paths, not serialized `FileScanTask`s: in iceberg 0.11 a planned task
//! always carries its partition data, and serializing that field is not implemented. Planning
//! the pinned snapshot again gives every shard complete tasks (delete files included) from the
//! library itself.
//!
//! Partial results are exact and order independent. Sums are kept as integers in micro-units
//! (1e-6), so the merged result does not depend on the number of shards or on the order in which
//! partial results are merged. It matches [`crate::stats::compute_trip_stats`] on the same rows.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch,
    StringArray, StringViewArray,
};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::AppError;
use crate::s3_uri::parse_s3_uri;

/// Columns read by the shards: the inputs of `compute_trip_stats`.
pub const STATS_COLUMNS: [&str; 6] = [
    "vendor_id",
    "passenger_count",
    "trip_distance",
    "payment_type",
    "fare_amount",
    "tip_amount",
];

/// Upper bound for `shard_count` (one Kubernetes pod per shard).
pub const MAX_SHARDS: usize = 1000;

/// Version of the JSON documents written under the run URI.
const FORMAT_VERSION: u32 = 1;

/// Fixed-point scale of the sums: values are stored as integer micro-units.
const MICROS_PER_UNIT: f64 = 1_000_000.0;

/// Largest magnitude converted to micro-units (keeps the sums far from `i64` overflow per value).
const MAX_ABS_VALUE: f64 = 1.0e12;

/// Partial results read concurrently by `merge_trip_stats`.
const MERGE_READ_CONCURRENCY: usize = 16;

fn distributed_err(msg: impl Into<String>) -> AppError {
    AppError::Distributed(msg.into())
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

fn to_micros(value: f64, column: &str) -> Result<i64, AppError> {
    if !value.is_finite() || value.abs() > MAX_ABS_VALUE {
        return Err(distributed_err(format!(
            "column {column} has a value that cannot be summed exactly: {value}"
        )));
    }
    Ok((value * MICROS_PER_UNIT).round() as i64)
}

fn checked_sum(a: i64, b: i64, what: &str) -> Result<i64, AppError> {
    a.checked_add(b)
        .ok_or_else(|| distributed_err(format!("overflow while summing {what}")))
}

fn add_count(map: &mut BTreeMap<String, u64>, key: &str, n: u64) {
    if let Some(count) = map.get_mut(key) {
        *count += n;
    } else {
        map.insert(key.to_owned(), n);
    }
}

// ---------------------------------------------------------------------------------------------
// Mergeable trip statistics
// ---------------------------------------------------------------------------------------------

/// Partial trip statistics of a set of rows. Two accumulators merge into the accumulator of the
/// union of their rows; [`TripStatsAccumulator::to_trip_stats`] gives the `compute_trip_stats`
/// JSON. NULL values (the shared table has none) are left out of sums, extremes and breakdowns.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TripStatsAccumulator {
    pub trips: u64,
    pub passengers: i64,
    pub distance_micros: i64,
    pub fare_micros: i64,
    pub tips_micros: i64,
    pub min_distance: Option<f64>,
    pub max_distance: Option<f64>,
    pub vendors: BTreeMap<String, u64>,
    pub payment_types: BTreeMap<String, u64>,
}

impl TripStatsAccumulator {
    fn add_distance_extremes(&mut self, distance: f64) {
        self.min_distance = Some(self.min_distance.map_or(distance, |m| m.min(distance)));
        self.max_distance = Some(self.max_distance.map_or(distance, |m| m.max(distance)));
    }

    /// Adds the rows of a batch that has (at least) the [`STATS_COLUMNS`].
    pub fn add_batch(&mut self, batch: &RecordBatch) -> Result<(), AppError> {
        let vendor = StrColumn::from_batch(batch, "vendor_id")?;
        let payment = StrColumn::from_batch(batch, "payment_type")?;
        let passengers = IntColumn::from_batch(batch, "passenger_count")?;
        let distance = f64_column(batch, "trip_distance")?;
        let fare = f64_column(batch, "fare_amount")?;
        let tip = f64_column(batch, "tip_amount")?;

        for row in 0..batch.num_rows() {
            self.trips += 1;
            if let Some(v) = vendor.value(row) {
                add_count(&mut self.vendors, v, 1);
            }
            if let Some(p) = payment.value(row) {
                add_count(&mut self.payment_types, p, 1);
            }
            if let Some(n) = passengers.value(row) {
                self.passengers = checked_sum(self.passengers, n, "passenger_count")?;
            }
            if !distance.is_null(row) {
                let d = distance.value(row);
                let micros = to_micros(d, "trip_distance")?;
                self.distance_micros = checked_sum(self.distance_micros, micros, "trip_distance")?;
                self.add_distance_extremes(d);
            }
            if !fare.is_null(row) {
                let micros = to_micros(fare.value(row), "fare_amount")?;
                self.fare_micros = checked_sum(self.fare_micros, micros, "fare_amount")?;
            }
            if !tip.is_null(row) {
                let micros = to_micros(tip.value(row), "tip_amount")?;
                self.tips_micros = checked_sum(self.tips_micros, micros, "tip_amount")?;
            }
        }
        Ok(())
    }

    /// Adds the rows counted by `other`.
    pub fn merge(&mut self, other: &TripStatsAccumulator) -> Result<(), AppError> {
        self.trips = self
            .trips
            .checked_add(other.trips)
            .ok_or_else(|| distributed_err("overflow while counting trips"))?;
        self.passengers = checked_sum(self.passengers, other.passengers, "passenger_count")?;
        self.distance_micros =
            checked_sum(self.distance_micros, other.distance_micros, "trip_distance")?;
        self.fare_micros = checked_sum(self.fare_micros, other.fare_micros, "fare_amount")?;
        self.tips_micros = checked_sum(self.tips_micros, other.tips_micros, "tip_amount")?;
        if let Some(m) = other.min_distance {
            self.min_distance = Some(self.min_distance.map_or(m, |s| s.min(m)));
        }
        if let Some(m) = other.max_distance {
            self.max_distance = Some(self.max_distance.map_or(m, |s| s.max(m)));
        }
        for (k, n) in &other.vendors {
            add_count(&mut self.vendors, k, *n);
        }
        for (k, n) in &other.payment_types {
            add_count(&mut self.payment_types, k, *n);
        }
        Ok(())
    }

    /// The statistics with the definitions and rounding of `compute_trip_stats` (`{}` when there
    /// are no rows).
    pub fn to_trip_stats(&self) -> Value {
        if self.trips == 0 {
            return serde_json::json!({});
        }
        let trips = self.trips as f64;
        let distance = self.distance_micros as f64;
        let fare = self.fare_micros as f64;
        let tips = self.tips_micros as f64;
        let tip_rate = if self.fare_micros > 0 {
            tips / fare * 100.0
        } else {
            0.0
        };
        serde_json::json!({
            "total_trips": self.trips,
            "total_passengers": self.passengers,
            "total_distance_miles": round2(distance / MICROS_PER_UNIT),
            "total_fare_usd": round2(fare / MICROS_PER_UNIT),
            "total_tips_usd": round2(tips / MICROS_PER_UNIT),
            "avg_distance_miles": round2(distance / trips / MICROS_PER_UNIT),
            "avg_fare_usd": round2(fare / trips / MICROS_PER_UNIT),
            "avg_tip_usd": round2(tips / trips / MICROS_PER_UNIT),
            "tip_rate_percent": round2(tip_rate),
            "max_distance_miles": self.max_distance,
            "min_distance_miles": self.min_distance,
            "vendor_breakdown": self.vendors,
            "payment_type_breakdown": self.payment_types,
        })
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, AppError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| distributed_err(format!("column {name} is missing from the scan")))
}

fn type_error(name: &str, col: &ArrayRef) -> AppError {
    distributed_err(format!(
        "column {name} has the unexpected Arrow type {}",
        col.data_type()
    ))
}

/// A string column in any of Arrow's three string layouts.
enum StrColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StrColumn<'a> {
    fn from_batch(batch: &'a RecordBatch, name: &str) -> Result<Self, AppError> {
        let col = column(batch, name)?;
        let any = col.as_any();
        if let Some(a) = any.downcast_ref::<StringArray>() {
            Ok(Self::Utf8(a))
        } else if let Some(a) = any.downcast_ref::<LargeStringArray>() {
            Ok(Self::LargeUtf8(a))
        } else if let Some(a) = any.downcast_ref::<StringViewArray>() {
            Ok(Self::Utf8View(a))
        } else {
            Err(type_error(name, col))
        }
    }

    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Utf8(a) => (!a.is_null(row)).then(|| a.value(row)),
            Self::LargeUtf8(a) => (!a.is_null(row)).then(|| a.value(row)),
            Self::Utf8View(a) => (!a.is_null(row)).then(|| a.value(row)),
        }
    }
}

/// An `int` or `long` column.
enum IntColumn<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl<'a> IntColumn<'a> {
    fn from_batch(batch: &'a RecordBatch, name: &str) -> Result<Self, AppError> {
        let col = column(batch, name)?;
        let any = col.as_any();
        if let Some(a) = any.downcast_ref::<Int32Array>() {
            Ok(Self::Int32(a))
        } else if let Some(a) = any.downcast_ref::<Int64Array>() {
            Ok(Self::Int64(a))
        } else {
            Err(type_error(name, col))
        }
    }

    fn value(&self, row: usize) -> Option<i64> {
        match self {
            Self::Int32(a) => (!a.is_null(row)).then(|| i64::from(a.value(row))),
            Self::Int64(a) => (!a.is_null(row)).then(|| a.value(row)),
        }
    }
}

fn f64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array, AppError> {
    let col = column(batch, name)?;
    col.as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| type_error(name, col))
}

// ---------------------------------------------------------------------------------------------
// Plan
// ---------------------------------------------------------------------------------------------

/// A data file of the pinned snapshot, as planned by iceberg.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedFile {
    pub path: String,
    pub size_bytes: u64,
    /// Rows in the data file before deletes (from the manifest).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_count: Option<u64>,
    /// Delete files that apply to the data file.
    pub delete_files: usize,
}

/// The data files assigned to one shard (one pod).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Shard {
    pub index: usize,
    pub bytes: u64,
    pub files: Vec<PlannedFile>,
}

/// `<run_uri>/plan.json`, written by `plan_trip_stats`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatsPlan {
    pub version: u32,
    pub run_uri: String,
    pub namespace: String,
    pub table: String,
    /// `None` when the table has no snapshot (no data yet): every shard is empty.
    pub snapshot_id: Option<i64>,
    pub columns: Vec<String>,
    pub created_at_ms: u64,
    pub shard_count: usize,
    pub total_files: usize,
    pub total_bytes: u64,
    pub shards: Vec<Shard>,
}

impl StatsPlan {
    /// Rows the shards must read in total, when the manifests give it: every file has a record
    /// count and no file has delete files.
    pub fn expected_rows(&self) -> Option<u64> {
        let files = self.shards.iter().flat_map(|s| &s.files);
        let mut total = 0u64;
        for f in files {
            if f.delete_files > 0 {
                return None;
            }
            total = total.checked_add(f.record_count?)?;
        }
        Some(total)
    }

    /// The plan without the file lists (for the response).
    pub fn summary(&self) -> Value {
        let shards: Vec<Value> = self
            .shards
            .iter()
            .map(
                |s| serde_json::json!({"index": s.index, "files": s.files.len(), "bytes": s.bytes}),
            )
            .collect();
        serde_json::json!({
            "run_uri": self.run_uri,
            "plan_uri": run_object_uri(&self.run_uri, PLAN_OBJECT),
            "namespace": self.namespace,
            "table": self.table,
            "snapshot_id": self.snapshot_id,
            "shard_count": self.shard_count,
            "total_files": self.total_files,
            "total_bytes": self.total_bytes,
            "expected_rows": self.expected_rows(),
            "shards": shards,
        })
    }
}

/// Checks `shard_count` (1 to [`MAX_SHARDS`]).
pub fn validate_shard_count(shard_count: usize) -> Result<(), AppError> {
    if shard_count == 0 || shard_count > MAX_SHARDS {
        return Err(AppError::Config(format!(
            "shard_count must be between 1 and {MAX_SHARDS}, got {shard_count}"
        )));
    }
    Ok(())
}

/// Assigns the files to `shard_count` shards, largest file first to the shard with the fewest
/// bytes (ties: fewer files, then lower index). Deterministic for the same files. Shards are
/// empty when there are fewer files than shards. Files are sorted by path inside each shard.
pub fn assign_shards(mut files: Vec<PlannedFile>, shard_count: usize) -> Vec<Shard> {
    files.sort_by(|a, b| {
        b.size_bytes
            .cmp(&a.size_bytes)
            .then_with(|| a.path.cmp(&b.path))
    });
    let mut shards: Vec<Shard> = (0..shard_count)
        .map(|index| Shard {
            index,
            bytes: 0,
            files: Vec::new(),
        })
        .collect();
    let mut heap: BinaryHeap<Reverse<(u64, usize, usize)>> =
        (0..shard_count).map(|i| Reverse((0, 0, i))).collect();
    for file in files {
        let Some(Reverse((bytes, count, index))) = heap.pop() else {
            break; // shard_count == 0: nothing to assign to
        };
        let size = file.size_bytes;
        shards[index].files.push(file);
        shards[index].bytes = bytes.saturating_add(size);
        heap.push(Reverse((shards[index].bytes, count + 1, index)));
    }
    for shard in &mut shards {
        shard.files.sort_by(|a, b| a.path.cmp(&b.path));
    }
    shards
}

async fn plan_tasks(
    table: &Table,
    snapshot_id: i64,
    columns: &[String],
) -> Result<Vec<FileScanTask>, AppError> {
    let scan = table
        .scan()
        .snapshot_id(snapshot_id)
        .select(columns.iter())
        .build()?;
    let tasks: Vec<FileScanTask> = scan.plan_files().await?.try_collect().await?;
    Ok(tasks)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Pins the current snapshot of `table`, plans its scan and splits the data files into shards.
pub async fn plan_trip_stats(
    table: &Table,
    namespace: &str,
    table_name: &str,
    run_uri: &str,
    shard_count: usize,
) -> Result<StatsPlan, AppError> {
    validate_shard_count(shard_count)?;
    let columns: Vec<String> = STATS_COLUMNS.iter().map(|c| c.to_string()).collect();
    let snapshot_id = table.metadata().current_snapshot_id();
    let files: Vec<PlannedFile> = match snapshot_id {
        None => Vec::new(),
        Some(id) => plan_tasks(table, id, &columns)
            .await?
            .iter()
            .map(|t| PlannedFile {
                path: t.data_file_path().to_string(),
                size_bytes: t.file_size_in_bytes(),
                record_count: t.record_count(),
                delete_files: t.deletes().len(),
            })
            .collect(),
    };
    let total_files = files.len();
    let total_bytes = files.iter().map(|f| f.size_bytes).sum();
    if total_files < shard_count {
        tracing::warn!(
            total_files,
            shard_count,
            "fewer data files than shards: some shards will be empty"
        );
    }
    Ok(StatsPlan {
        version: FORMAT_VERSION,
        run_uri: run_uri.to_string(),
        namespace: namespace.to_string(),
        table: table_name.to_string(),
        snapshot_id,
        columns,
        created_at_ms: now_ms(),
        shard_count,
        total_files,
        total_bytes,
        shards: assign_shards(files, shard_count),
    })
}

// ---------------------------------------------------------------------------------------------
// Map: one shard
// ---------------------------------------------------------------------------------------------

/// `<run_uri>/shards/shard-<index>.json`, written by `trip_stats_shard`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardResult {
    pub version: u32,
    pub run_uri: String,
    pub shard_index: usize,
    pub shard_count: usize,
    pub snapshot_id: Option<i64>,
    pub files_read: usize,
    pub rows_read: u64,
    pub bytes_planned: u64,
    pub elapsed_ms: u64,
    pub stats: TripStatsAccumulator,
}

impl ShardResult {
    pub fn summary(&self) -> Value {
        serde_json::json!({
            "run_uri": self.run_uri,
            "result_uri": run_object_uri(&self.run_uri, &shard_object(self.shard_index)),
            "shard_index": self.shard_index,
            "shard_count": self.shard_count,
            "snapshot_id": self.snapshot_id,
            "files_read": self.files_read,
            "rows_read": self.rows_read,
            "bytes_planned": self.bytes_planned,
            "elapsed_ms": self.elapsed_ms,
            "trip_stats": self.stats.to_trip_stats(),
        })
    }
}

/// Keeps the items whose path is one of `wanted`. Fails unless every wanted path matches exactly
/// one item (the snapshot was planned again, so it must contain exactly the planned files).
pub fn select_by_path<T>(
    items: Vec<T>,
    path: impl Fn(&T) -> &str,
    wanted: &[PlannedFile],
) -> Result<Vec<T>, AppError> {
    let mut found: HashMap<&str, usize> = wanted.iter().map(|f| (f.path.as_str(), 0)).collect();
    let mut selected = Vec::with_capacity(wanted.len());
    for item in items {
        if let Some(n) = found.get_mut(path(&item)) {
            *n += 1;
            selected.push(item);
        }
    }
    let mut bad: Vec<String> = found
        .iter()
        .filter(|(_, n)| **n != 1)
        .map(|(p, n)| format!("{p} (found {n} times)"))
        .collect();
    if !bad.is_empty() {
        bad.sort();
        let shown = bad.len().min(5);
        return Err(distributed_err(format!(
            "the planned snapshot does not match the plan for {} file(s): {}",
            bad.len(),
            bad[..shown].join(", ")
        )));
    }
    Ok(selected)
}

/// Reads the files of shard `index` and returns its partial statistics.
pub async fn run_trip_stats_shard(
    table: &Table,
    plan: &StatsPlan,
    index: usize,
) -> Result<ShardResult, AppError> {
    let shard = plan.shards.get(index).ok_or_else(|| {
        AppError::Config(format!(
            "shard index {index} is out of range: the plan has {} shards",
            plan.shard_count
        ))
    })?;
    let started = Instant::now();
    let mut stats = TripStatsAccumulator::default();
    let mut files_read = 0;

    if !shard.files.is_empty() {
        let snapshot_id = plan
            .snapshot_id
            .ok_or_else(|| distributed_err("the plan has files but no snapshot id"))?;
        let tasks = plan_tasks(table, snapshot_id, &plan.columns).await?;
        let tasks = select_by_path(tasks, |t| t.data_file_path(), &shard.files)?;
        files_read = tasks.len();
        tracing::info!(
            shard = index,
            files = files_read,
            bytes = shard.bytes,
            snapshot_id,
            "reading shard"
        );

        let reader = table.reader_builder().build();
        let mut batches = reader
            .read(stream::iter(tasks.into_iter().map(Ok)).boxed())?
            .stream();
        while let Some(batch) = batches.try_next().await? {
            stats.add_batch(&batch)?;
        }
    }

    Ok(ShardResult {
        version: FORMAT_VERSION,
        run_uri: plan.run_uri.clone(),
        shard_index: index,
        shard_count: plan.shard_count,
        snapshot_id: plan.snapshot_id,
        files_read,
        rows_read: stats.trips,
        bytes_planned: shard.bytes,
        elapsed_ms: started.elapsed().as_millis() as u64,
        stats,
    })
}

/// Shard index of this pod: `JOB_COMPLETION_INDEX`, set by Kubernetes in Indexed Jobs.
pub fn shard_index_from_env(value: Option<String>) -> Result<usize, AppError> {
    let value = value.ok_or(AppError::MissingField("shard_index"))?;
    value.trim().parse().map_err(|_| {
        AppError::Config(format!(
            "JOB_COMPLETION_INDEX must be a non-negative integer, got '{value}'"
        ))
    })
}

// ---------------------------------------------------------------------------------------------
// Reduce
// ---------------------------------------------------------------------------------------------

/// Checks the partial results against the plan and merges them. `results` must hold one result
/// per shard, in shard order.
pub fn merge_shard_results(plan: &StatsPlan, results: &[ShardResult]) -> Result<Value, AppError> {
    if results.len() != plan.shard_count {
        return Err(distributed_err(format!(
            "expected {} shard results, got {}",
            plan.shard_count,
            results.len()
        )));
    }
    let mut total = TripStatsAccumulator::default();
    let mut shards = Vec::with_capacity(results.len());
    for (i, (shard, r)) in plan.shards.iter().zip(results).enumerate() {
        let mismatch = |what: &str| {
            distributed_err(format!(
                "shard {i}: {what} does not match the plan (stale or foreign result?)"
            ))
        };
        if r.shard_index != i {
            return Err(mismatch("shard_index"));
        }
        if r.run_uri != plan.run_uri {
            return Err(mismatch("run_uri"));
        }
        if r.shard_count != plan.shard_count {
            return Err(mismatch("shard_count"));
        }
        if r.snapshot_id != plan.snapshot_id {
            return Err(mismatch("snapshot_id"));
        }
        if r.files_read != shard.files.len() {
            return Err(distributed_err(format!(
                "shard {i} read {} files, the plan has {}",
                r.files_read,
                shard.files.len()
            )));
        }
        total.merge(&r.stats)?;
        shards.push(serde_json::json!({
            "index": i,
            "files": r.files_read,
            "bytes": shard.bytes,
            "rows": r.rows_read,
            "elapsed_ms": r.elapsed_ms,
        }));
    }
    if let Some(expected) = plan.expected_rows()
        && total.trips != expected
    {
        return Err(distributed_err(format!(
            "the shards read {} rows, the manifests list {expected}",
            total.trips
        )));
    }
    Ok(serde_json::json!({
        "run_uri": plan.run_uri,
        "namespace": plan.namespace,
        "table": plan.table,
        "snapshot_id": plan.snapshot_id,
        "shard_count": plan.shard_count,
        "files": plan.total_files,
        "bytes": plan.total_bytes,
        "rows_read": total.trips,
        "trip_stats": total.to_trip_stats(),
        "shards": shards,
    }))
}

// ---------------------------------------------------------------------------------------------
// Run documents on S3
// ---------------------------------------------------------------------------------------------

pub const PLAN_OBJECT: &str = "plan.json";
pub const RESULT_OBJECT: &str = "result.json";

pub fn shard_object(index: usize) -> String {
    format!("shards/shard-{index:05}.json")
}

/// Checks that `run_uri` is `s3://<bucket>/<prefix>` with a prefix and no trailing slash (an
/// empty run id in `.../runs/$(RUN_ID)` gives a trailing slash).
pub fn validate_run_uri(run_uri: &str) -> Result<(), AppError> {
    let valid =
        matches!(parse_s3_uri(run_uri), Some((_, key)) if !key.is_empty() && !key.ends_with('/'));
    if !valid {
        return Err(AppError::Config(format!(
            "run_uri must be s3://<bucket>/<prefix>/<run id> without a trailing slash, got '{run_uri}'"
        )));
    }
    Ok(())
}

fn run_object_uri(run_uri: &str, name: &str) -> String {
    format!("{run_uri}/{name}")
}

fn run_object_location(run_uri: &str, name: &str) -> Result<(String, String), AppError> {
    validate_run_uri(run_uri)?;
    let (bucket, prefix) = parse_s3_uri(run_uri).expect("validated above");
    Ok((bucket.to_string(), format!("{prefix}/{name}")))
}

async fn put_json<T: Serialize>(
    s3: &S3Client,
    run_uri: &str,
    name: &str,
    value: &T,
    only_if_absent: bool,
) -> Result<String, AppError> {
    let (bucket, key) = run_object_location(run_uri, name)?;
    let uri = run_object_uri(run_uri, name);
    let body = serde_json::to_vec_pretty(value)
        .map_err(|e| distributed_err(format!("cannot serialize {uri}: {e}")))?;
    let mut put = s3
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .content_type("application/json")
        .body(ByteStream::from(body));
    if only_if_absent {
        put = put.if_none_match("*");
    }
    match put.send().await {
        Ok(_) => Ok(uri),
        Err(e) if e.as_service_error().and_then(|se| se.code()) == Some("PreconditionFailed") => {
            Err(distributed_err(format!(
                "{uri} already exists: use a new run id"
            )))
        }
        Err(e) => Err(e.into()),
    }
}

async fn get_json<T: DeserializeOwned>(
    s3: &S3Client,
    run_uri: &str,
    name: &str,
) -> Result<Option<T>, AppError> {
    let (bucket, key) = run_object_location(run_uri, name)?;
    let uri = run_object_uri(run_uri, name);
    let resp = match s3.get_object().bucket(&bucket).key(&key).send().await {
        Ok(resp) => resp,
        Err(e) if e.as_service_error().is_some_and(|se| se.is_no_such_key()) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let bytes = resp.body.collect().await?.into_bytes();
    let value = serde_json::from_slice(&bytes)
        .map_err(|e| distributed_err(format!("{uri} is not a valid document: {e}")))?;
    Ok(Some(value))
}

/// Writes `<run_uri>/plan.json`; fails if it already exists.
pub async fn write_plan(s3: &S3Client, plan: &StatsPlan) -> Result<String, AppError> {
    put_json(s3, &plan.run_uri, PLAN_OBJECT, plan, true).await
}

/// Reads `<run_uri>/plan.json`.
pub async fn read_plan(s3: &S3Client, run_uri: &str) -> Result<StatsPlan, AppError> {
    let plan: StatsPlan = get_json(s3, run_uri, PLAN_OBJECT).await?.ok_or_else(|| {
        distributed_err(format!(
            "{} not found: run plan_trip_stats first",
            run_object_uri(run_uri, PLAN_OBJECT)
        ))
    })?;
    if plan.version != FORMAT_VERSION || plan.run_uri != run_uri {
        return Err(distributed_err(format!(
            "{} is not a plan of this run (version {}, run_uri {})",
            run_object_uri(run_uri, PLAN_OBJECT),
            plan.version,
            plan.run_uri
        )));
    }
    if plan.shards.len() != plan.shard_count {
        return Err(distributed_err(
            "the plan's shard list does not match shard_count",
        ));
    }
    Ok(plan)
}

/// Writes `<run_uri>/shards/shard-<index>.json` (a retried pod overwrites its own result).
pub async fn write_shard_result(s3: &S3Client, result: &ShardResult) -> Result<String, AppError> {
    put_json(
        s3,
        &result.run_uri,
        &shard_object(result.shard_index),
        result,
        false,
    )
    .await
}

/// Reads the result of every shard of `plan`; fails naming the shards that have none.
pub async fn read_shard_results(
    s3: &S3Client,
    plan: &StatsPlan,
) -> Result<Vec<ShardResult>, AppError> {
    let results: Vec<Option<ShardResult>> = stream::iter(0..plan.shard_count)
        .map(|i| async move { get_json::<ShardResult>(s3, &plan.run_uri, &shard_object(i)).await })
        .buffered(MERGE_READ_CONCURRENCY)
        .try_collect()
        .await?;
    let missing: Vec<usize> = results
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.is_none().then_some(i))
        .collect();
    if !missing.is_empty() {
        return Err(distributed_err(format!(
            "no result yet for {} of {} shards: {:?}",
            missing.len(),
            plan.shard_count,
            missing
        )));
    }
    Ok(results.into_iter().flatten().collect())
}

/// Writes the merged result to `<run_uri>/result.json`.
pub async fn write_result(
    s3: &S3Client,
    run_uri: &str,
    merged: &Value,
) -> Result<String, AppError> {
    put_json(s3, run_uri, RESULT_OBJECT, merged, false).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{RowInput, sample_payload, sample_row};
    use crate::stats::compute_trip_stats;
    use crate::writer::build_record_batch;
    use std::sync::Arc;

    fn accumulate(rows: &[RowInput]) -> TripStatsAccumulator {
        let mut acc = TripStatsAccumulator::default();
        acc.add_batch(&build_record_batch(rows).unwrap()).unwrap();
        acc
    }

    fn file(path: &str, size: u64, records: u64) -> PlannedFile {
        PlannedFile {
            path: path.to_string(),
            size_bytes: size,
            record_count: Some(records),
            delete_files: 0,
        }
    }

    fn plan_of(files: Vec<PlannedFile>, shard_count: usize) -> StatsPlan {
        let total_files = files.len();
        let total_bytes = files.iter().map(|f| f.size_bytes).sum();
        StatsPlan {
            version: FORMAT_VERSION,
            run_uri: "s3://b/lab/runs/r1".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            snapshot_id: Some(42),
            columns: STATS_COLUMNS.iter().map(|c| c.to_string()).collect(),
            created_at_ms: 0,
            shard_count,
            total_files,
            total_bytes,
            shards: assign_shards(files, shard_count),
        }
    }

    fn result_of(plan: &StatsPlan, index: usize, stats: TripStatsAccumulator) -> ShardResult {
        ShardResult {
            version: FORMAT_VERSION,
            run_uri: plan.run_uri.clone(),
            shard_index: index,
            shard_count: plan.shard_count,
            snapshot_id: plan.snapshot_id,
            files_read: plan.shards[index].files.len(),
            rows_read: stats.trips,
            bytes_planned: plan.shards[index].bytes,
            elapsed_ms: 1,
            stats,
        }
    }

    #[test]
    fn test_accumulator_matches_compute_trip_stats_on_sample_payload() {
        let rows = sample_payload().rows;
        assert_eq!(accumulate(&rows).to_trip_stats(), compute_trip_stats(&rows));
    }

    #[test]
    fn test_merge_is_exact_and_independent_of_split_and_order() {
        let rows = sample_payload().rows;
        let whole = accumulate(&rows);
        for chunk_size in [1, 7, 333, 1250, 4999] {
            let parts: Vec<TripStatsAccumulator> =
                rows.chunks(chunk_size).map(accumulate).collect();
            let mut forward = TripStatsAccumulator::default();
            for p in &parts {
                forward.merge(p).unwrap();
            }
            let mut backward = TripStatsAccumulator::default();
            for p in parts.iter().rev() {
                backward.merge(p).unwrap();
            }
            assert_eq!(forward, whole, "chunk size {chunk_size}");
            assert_eq!(backward, whole, "chunk size {chunk_size}");
        }
    }

    #[test]
    fn test_merging_copies_scales_the_totals() {
        // The shared table after the Spark load plus two Rust appends: 3 copies of the payload.
        let one = accumulate(&sample_payload().rows);
        let mut three = TripStatsAccumulator::default();
        for _ in 0..3 {
            three.merge(&one).unwrap();
        }
        assert_eq!(three.trips, 15_000);
        assert_eq!(three.distance_micros, 3 * one.distance_micros);
        let stats = three.to_trip_stats();
        assert_eq!(stats["total_trips"], 15_000);
        assert_eq!(stats["vendor_breakdown"]["CMT"], 3 * 2432);
        assert_eq!(stats["vendor_breakdown"]["VTS"], 3 * 2568);
        assert_eq!(stats["tip_rate_percent"], 11.67);
        assert_eq!(stats["total_distance_miles"], 43_997.58);
        assert_eq!(stats["total_fare_usd"], 187_224.0);
        assert_eq!(stats["total_tips_usd"], 21_840.93);
        assert_eq!(stats["min_distance_miles"], 0.01);
        assert_eq!(stats["max_distance_miles"], 32.2);
    }

    #[test]
    fn test_empty_accumulator_gives_empty_stats() {
        let mut acc = TripStatsAccumulator::default();
        assert_eq!(acc.to_trip_stats(), serde_json::json!({}));
        acc.merge(&TripStatsAccumulator::default()).unwrap();
        assert_eq!(acc, TripStatsAccumulator::default());
    }

    #[test]
    fn test_accumulator_round_trips_through_json() {
        let acc = accumulate(&sample_payload().rows[..10]);
        let json = serde_json::to_string(&acc).unwrap();
        let back: TripStatsAccumulator = serde_json::from_str(&json).unwrap();
        assert_eq!(back, acc);
    }

    #[test]
    fn test_add_batch_rejects_non_finite_values_and_missing_columns() {
        let mut row = sample_row();
        row.fare_amount = f64::NAN;
        let mut acc = TripStatsAccumulator::default();
        let err = acc
            .add_batch(&build_record_batch(&[row]).unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("fare_amount"), "{err}");

        let batch = build_record_batch(&[sample_row()]).unwrap();
        let without_tip = batch.project(&[0, 1, 2, 7, 9, 14, 15]).unwrap();
        let err = TripStatsAccumulator::default()
            .add_batch(&without_tip)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "distributed stats error: column tip_amount is missing from the scan"
        );
    }

    #[test]
    fn test_add_batch_reads_string_views_and_long_passenger_counts() {
        use arrow_schema::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("vendor_id", DataType::Utf8View, true),
            Field::new("passenger_count", DataType::Int64, true),
            Field::new("trip_distance", DataType::Float64, true),
            Field::new("payment_type", DataType::LargeUtf8, true),
            Field::new("fare_amount", DataType::Float64, true),
            Field::new("tip_amount", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec![Some("CMT"), None])),
                Arc::new(Int64Array::from(vec![Some(2), Some(3)])),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
                Arc::new(LargeStringArray::from(vec![Some("CRD"), Some("CSH")])),
                Arc::new(Float64Array::from(vec![Some(10.0), Some(5.0)])),
                Arc::new(Float64Array::from(vec![Some(2.0), None])),
            ],
        )
        .unwrap();
        let mut acc = TripStatsAccumulator::default();
        acc.add_batch(&batch).unwrap();
        assert_eq!(acc.trips, 2);
        assert_eq!(acc.passengers, 5);
        assert_eq!(acc.vendors, BTreeMap::from([("CMT".to_string(), 1)]));
        assert_eq!(acc.payment_types.len(), 2);
        assert_eq!(acc.distance_micros, 1_500_000);
        assert_eq!((acc.min_distance, acc.max_distance), (Some(1.5), Some(1.5)));
        assert_eq!(acc.fare_micros, 15_000_000);
        assert_eq!(acc.tips_micros, 2_000_000);
    }

    #[test]
    fn test_assign_shards_balances_by_size_and_is_deterministic() {
        let files = vec![
            file("s3://b/d/a", 100, 1),
            file("s3://b/d/b", 90, 1),
            file("s3://b/d/c", 50, 1),
            file("s3://b/d/d", 40, 1),
            file("s3://b/d/e", 10, 1),
        ];
        let shards = assign_shards(files.clone(), 2);
        let bytes: Vec<u64> = shards.iter().map(|s| s.bytes).collect();
        assert_eq!(bytes, [150, 140]);
        let mut reversed = files.clone();
        reversed.reverse();
        assert_eq!(assign_shards(reversed, 2), shards);

        // Every file exactly once, files sorted by path inside each shard.
        let mut all: Vec<&str> = shards
            .iter()
            .flat_map(|s| s.files.iter().map(|f| f.path.as_str()))
            .collect();
        for s in &shards {
            assert!(s.files.windows(2).all(|w| w[0].path < w[1].path));
        }
        all.sort();
        assert_eq!(all.len(), 5);
        all.dedup();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn test_assign_shards_leaves_extra_shards_empty() {
        let shards = assign_shards(vec![file("s3://b/x", 5, 1), file("s3://b/y", 5, 1)], 4);
        let counts: Vec<usize> = shards.iter().map(|s| s.files.len()).collect();
        assert_eq!(counts, [1, 1, 0, 0]);
        assert_eq!(
            shards.iter().map(|s| s.index).collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        assert!(
            assign_shards(Vec::new(), 3)
                .iter()
                .all(|s| s.files.is_empty())
        );
    }

    #[test]
    fn test_validate_shard_count() {
        assert!(validate_shard_count(1).is_ok());
        assert!(validate_shard_count(MAX_SHARDS).is_ok());
        for n in [0, MAX_SHARDS + 1] {
            assert!(matches!(validate_shard_count(n), Err(AppError::Config(_))));
        }
    }

    #[test]
    fn test_select_by_path_keeps_exactly_the_planned_files() {
        let wanted = [file("s3://b/2", 1, 1), file("s3://b/4", 1, 1)];
        let items = vec!["s3://b/1", "s3://b/2", "s3://b/3", "s3://b/4"];
        assert_eq!(
            select_by_path(items, |s| s, &wanted).unwrap(),
            ["s3://b/2", "s3://b/4"]
        );

        let err = select_by_path(vec!["s3://b/2"], |s| s, &wanted).unwrap_err();
        assert!(
            err.to_string().contains("s3://b/4 (found 0 times)"),
            "{err}"
        );
        let err =
            select_by_path(vec!["s3://b/2", "s3://b/2", "s3://b/4"], |s| s, &wanted).unwrap_err();
        assert!(
            err.to_string().contains("s3://b/2 (found 2 times)"),
            "{err}"
        );
    }

    #[test]
    fn test_expected_rows_needs_counts_and_no_deletes() {
        let plan = plan_of(vec![file("s3://b/a", 1, 10), file("s3://b/b", 1, 5)], 2);
        assert_eq!(plan.expected_rows(), Some(15));

        let mut with_deletes = plan.clone();
        with_deletes.shards[0].files[0].delete_files = 1;
        assert_eq!(with_deletes.expected_rows(), None);

        let mut without_count = plan.clone();
        without_count.shards[1].files[0].record_count = None;
        assert_eq!(without_count.expected_rows(), None);
    }

    #[test]
    fn test_plan_round_trips_and_summary_omits_file_lists() {
        let plan = plan_of(vec![file("s3://b/a", 7, 3)], 2);
        let back: StatsPlan = serde_json::from_str(&serde_json::to_string(&plan).unwrap()).unwrap();
        assert_eq!(back, plan);
        let summary = plan.summary();
        assert_eq!(summary["plan_uri"], "s3://b/lab/runs/r1/plan.json");
        assert_eq!(summary["expected_rows"], 3);
        assert_eq!(
            summary["shards"],
            serde_json::json!([{"index": 0, "files": 1, "bytes": 7}, {"index": 1, "files": 0, "bytes": 0}])
        );
    }

    #[test]
    fn test_merge_shard_results_checks_the_plan_and_merges() {
        let rows = sample_payload().rows;
        let plan = plan_of(
            vec![file("s3://b/a", 10, 2000), file("s3://b/b", 9, 3000)],
            3,
        );
        // assign_shards: a -> shard 0, b -> shard 1, shard 2 empty.
        let results = vec![
            result_of(&plan, 0, accumulate(&rows[..2000])),
            result_of(&plan, 1, accumulate(&rows[2000..])),
            result_of(&plan, 2, TripStatsAccumulator::default()),
        ];
        let merged = merge_shard_results(&plan, &results).unwrap();
        assert_eq!(merged["rows_read"], 5000);
        assert_eq!(merged["trip_stats"], compute_trip_stats(&rows));
        assert_eq!(merged["shards"][2]["rows"], 0);

        assert!(merge_shard_results(&plan, &results[..2]).is_err());

        let mut stale = results.clone();
        stale[1].snapshot_id = Some(7);
        let err = merge_shard_results(&plan, &stale).unwrap_err();
        assert!(err.to_string().contains("shard 1: snapshot_id"), "{err}");

        let mut partial = results.clone();
        partial[0].files_read = 0;
        let err = merge_shard_results(&plan, &partial).unwrap_err();
        assert!(err.to_string().contains("shard 0 read 0 files"), "{err}");

        let mut short = results.clone();
        short[1] = result_of(&plan, 1, accumulate(&rows[2000..4000]));
        let err = merge_shard_results(&plan, &short).unwrap_err();
        assert!(
            err.to_string()
                .contains("read 4000 rows, the manifests list 5000"),
            "{err}"
        );
    }

    #[test]
    fn test_validate_run_uri() {
        assert!(validate_run_uri("s3://b/lab/runs/r1").is_ok());
        for bad in ["", "b/lab/runs/r1", "s3://b", "s3://b/", "s3://b/lab/runs/"] {
            assert!(
                matches!(validate_run_uri(bad), Err(AppError::Config(_))),
                "{bad}"
            );
        }
        assert_eq!(
            run_object_location("s3://b/lab/runs/r1", &shard_object(3)).unwrap(),
            (
                "b".to_string(),
                "lab/runs/r1/shards/shard-00003.json".to_string()
            )
        );
    }

    #[test]
    fn test_shard_index_from_env() {
        assert_eq!(shard_index_from_env(Some("3".to_string())).unwrap(), 3);
        assert!(matches!(
            shard_index_from_env(None),
            Err(AppError::MissingField("shard_index"))
        ));
        assert!(matches!(
            shard_index_from_env(Some("-1".to_string())),
            Err(AppError::Config(_))
        ));
    }
}
