use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::error::AppError;
use crate::models::RowInput;

/// Builds the NYC Taxi Arrow schema with Parquet field-id metadata.
pub fn build_arrow_schema() -> Arc<ArrowSchema> {
    let field_id =
        |id: &str| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);

    Arc::new(ArrowSchema::new(vec![
        Field::new("medallion", DataType::Utf8, false).with_metadata(field_id("1")),
        Field::new("hack_license", DataType::Utf8, false).with_metadata(field_id("2")),
        Field::new("vendor_id", DataType::Utf8, false).with_metadata(field_id("3")),
        Field::new("rate_code", DataType::Int32, false).with_metadata(field_id("4")),
        Field::new("store_and_fwd_flag", DataType::Utf8, false).with_metadata(field_id("5")),
        Field::new("pickup_datetime", DataType::Utf8, false).with_metadata(field_id("6")),
        Field::new("dropoff_datetime", DataType::Utf8, false).with_metadata(field_id("7")),
        Field::new("passenger_count", DataType::Int32, false).with_metadata(field_id("8")),
        Field::new("trip_time_in_secs", DataType::Int64, false).with_metadata(field_id("9")),
        Field::new("trip_distance", DataType::Float64, false).with_metadata(field_id("10")),
        Field::new("pickup_longitude", DataType::Float64, false).with_metadata(field_id("11")),
        Field::new("pickup_latitude", DataType::Float64, false).with_metadata(field_id("12")),
        Field::new("dropoff_longitude", DataType::Float64, false).with_metadata(field_id("13")),
        Field::new("dropoff_latitude", DataType::Float64, false).with_metadata(field_id("14")),
        Field::new("payment_type", DataType::Utf8, false).with_metadata(field_id("15")),
        Field::new("fare_amount", DataType::Float64, false).with_metadata(field_id("16")),
        Field::new("surcharge", DataType::Float64, false).with_metadata(field_id("17")),
        Field::new("mta_tax", DataType::Float64, false).with_metadata(field_id("18")),
        Field::new("tolls_amount", DataType::Float64, false).with_metadata(field_id("19")),
        Field::new("total_amount", DataType::Float64, false).with_metadata(field_id("20")),
        Field::new("tip_amount", DataType::Float64, false).with_metadata(field_id("21")),
        Field::new("tipped", DataType::Int32, false).with_metadata(field_id("22")),
        Field::new("tip_class", DataType::Int32, false).with_metadata(field_id("23")),
    ]))
}

/// Converts a slice of RowInput into an Arrow RecordBatch.
pub fn build_record_batch(rows: &[RowInput]) -> Result<RecordBatch, AppError> {
    let arrow_schema = build_arrow_schema();

    let medallion: StringArray = rows.iter().map(|r| Some(r.medallion.as_str())).collect();
    let hack_license: StringArray = rows.iter().map(|r| Some(r.hack_license.as_str())).collect();
    let vendor_id: StringArray = rows.iter().map(|r| Some(r.vendor_id.as_str())).collect();
    let rate_code: Int32Array = rows.iter().map(|r| Some(r.rate_code)).collect();
    let store_and_fwd_flag: StringArray = rows
        .iter()
        .map(|r| Some(r.store_and_fwd_flag.as_str()))
        .collect();
    let pickup_datetime: StringArray = rows
        .iter()
        .map(|r| Some(r.pickup_datetime.as_str()))
        .collect();
    let dropoff_datetime: StringArray = rows
        .iter()
        .map(|r| Some(r.dropoff_datetime.as_str()))
        .collect();
    let passenger_count: Int32Array = rows.iter().map(|r| Some(r.passenger_count)).collect();
    let trip_time_in_secs: Int64Array = rows.iter().map(|r| Some(r.trip_time_in_secs)).collect();
    let trip_distance: Float64Array = rows.iter().map(|r| Some(r.trip_distance)).collect();
    let pickup_longitude: Float64Array = rows.iter().map(|r| Some(r.pickup_longitude)).collect();
    let pickup_latitude: Float64Array = rows.iter().map(|r| Some(r.pickup_latitude)).collect();
    let dropoff_longitude: Float64Array = rows.iter().map(|r| Some(r.dropoff_longitude)).collect();
    let dropoff_latitude: Float64Array = rows.iter().map(|r| Some(r.dropoff_latitude)).collect();
    let payment_type: StringArray = rows.iter().map(|r| Some(r.payment_type.as_str())).collect();
    let fare_amount: Float64Array = rows.iter().map(|r| Some(r.fare_amount)).collect();
    let surcharge: Float64Array = rows.iter().map(|r| Some(r.surcharge)).collect();
    let mta_tax: Float64Array = rows.iter().map(|r| Some(r.mta_tax)).collect();
    let tolls_amount: Float64Array = rows.iter().map(|r| Some(r.tolls_amount)).collect();
    let total_amount: Float64Array = rows.iter().map(|r| Some(r.total_amount)).collect();
    let tip_amount: Float64Array = rows.iter().map(|r| Some(r.tip_amount)).collect();
    let tipped: Int32Array = rows.iter().map(|r| Some(r.tipped)).collect();
    let tip_class: Int32Array = rows.iter().map(|r| Some(r.tip_class)).collect();

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(medallion),
            Arc::new(hack_license),
            Arc::new(vendor_id),
            Arc::new(rate_code),
            Arc::new(store_and_fwd_flag),
            Arc::new(pickup_datetime),
            Arc::new(dropoff_datetime),
            Arc::new(passenger_count),
            Arc::new(trip_time_in_secs),
            Arc::new(trip_distance),
            Arc::new(pickup_longitude),
            Arc::new(pickup_latitude),
            Arc::new(dropoff_longitude),
            Arc::new(dropoff_latitude),
            Arc::new(payment_type),
            Arc::new(fare_amount),
            Arc::new(surcharge),
            Arc::new(mta_tax),
            Arc::new(tolls_amount),
            Arc::new(total_amount),
            Arc::new(tip_amount),
            Arc::new(tipped),
            Arc::new(tip_class),
        ],
    )?;

    Ok(batch)
}

/// Writes a RecordBatch as Parquet data files of `table`, without committing them.
///
/// The Parquet settings come from the table's `write.parquet.*` properties, through iceberg's
/// `ParquetWriterBuilder::from_table_properties`. That covers compression, row group size, page
/// size and dictionary size. Compression is zstd level 3 unless the table sets
/// `write.parquet.compression-codec` or `write.parquet.compression-level`, the same default as
/// Iceberg Java (Spark). Plain `WriterProperties` defaults would write uncompressed files.
pub async fn write_data_files(
    table: &Table,
    batch: RecordBatch,
) -> Result<Vec<DataFile>, AppError> {
    let table_props = table.metadata().table_properties();
    let compression = table_props.parquet_compression_codec()?;
    tracing::info!(%compression, rows = batch.num_rows(), "writing Parquet data files");

    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    let file_uuid = uuid::Uuid::new_v4().to_string();
    let file_name_generator = DefaultFileNameGenerator::new(
        "data".to_string(),
        Some(file_uuid),
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::from_table_properties(
        &table_props,
        table.metadata().current_schema().clone(),
    )?;

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut writer = data_file_writer_builder.build(None).await?;

    writer.write(batch).await?;
    Ok(writer.close().await?)
}

/// Writes a RecordBatch to Iceberg as Parquet and commits a fast-append transaction.
/// Returns the list of file paths written.
pub async fn commit_batch(
    catalog: &impl Catalog,
    table: &Table,
    batch: RecordBatch,
) -> Result<Vec<String>, AppError> {
    let data_files = write_data_files(table, batch).await?;

    let file_paths: Vec<String> = data_files
        .iter()
        .map(|df| df.file_path().to_string())
        .collect();

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;

    Ok(file_paths)
}

/// Writes rows as a Parquet file and commits a fast-append transaction.
pub async fn write_rows(
    catalog: &impl Catalog,
    table: &Table,
    rows: &[RowInput],
) -> Result<(), AppError> {
    let batch = build_record_batch(rows)?;
    commit_batch(catalog, table, batch).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int32Type, Int64Type};

    use crate::models::{sample_payload, sample_row};

    #[test]
    fn test_build_arrow_schema_has_23_non_nullable_fields_with_sequential_ids() {
        let schema = build_arrow_schema();

        assert_eq!(schema.fields().len(), 23);
        for (i, field) in schema.fields().iter().enumerate() {
            assert!(
                !field.is_nullable(),
                "{} should be non-nullable",
                field.name()
            );
            assert_eq!(
                field.metadata().get(PARQUET_FIELD_ID_META_KEY),
                Some(&(i + 1).to_string()),
                "field id of {}",
                field.name()
            );
        }
    }

    #[test]
    fn test_build_arrow_schema_column_types() {
        let schema = build_arrow_schema();
        let type_of = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();

        assert_eq!(type_of("medallion"), DataType::Utf8);
        assert_eq!(type_of("rate_code"), DataType::Int32);
        assert_eq!(type_of("trip_time_in_secs"), DataType::Int64);
        assert_eq!(type_of("trip_distance"), DataType::Float64);
        assert_eq!(type_of("payment_type"), DataType::Utf8);
        assert_eq!(type_of("tip_class"), DataType::Int32);
    }

    #[test]
    fn test_build_record_batch_maps_row_values() {
        let mut second = sample_row();
        second.vendor_id = "VTS".to_string();
        second.trip_time_in_secs = 1200;
        second.fare_amount = 12.25;
        second.tip_class = 2;
        let rows = vec![sample_row(), second];

        let batch = build_record_batch(&rows).unwrap();
        let col = |name: &str| batch.column_by_name(name).unwrap().clone();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 23);
        assert_eq!(col("vendor_id").as_string::<i32>().value(0), "CMT");
        assert_eq!(col("vendor_id").as_string::<i32>().value(1), "VTS");
        assert_eq!(
            col("trip_time_in_secs")
                .as_primitive::<Int64Type>()
                .values()
                .as_ref(),
            &[316, 1200]
        );
        assert_eq!(
            col("fare_amount")
                .as_primitive::<Float64Type>()
                .values()
                .as_ref(),
            &[5.5, 12.25]
        );
        assert_eq!(
            col("tip_class")
                .as_primitive::<Int32Type>()
                .values()
                .as_ref(),
            &[0, 2]
        );
    }

    #[test]
    fn test_build_record_batch_with_no_rows_is_empty() {
        let batch = build_record_batch(&[]).unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 23);
    }

    /// An unpartitioned v2 table with the NYC Taxi schema and `properties`, on in-memory storage.
    /// Call it inside a tokio runtime (the table borrows the current one).
    fn memory_table(properties: &[(&str, &str)]) -> Table {
        use iceberg::TableIdent;
        use iceberg::io::FileIO;
        use iceberg::spec::{FormatVersion, PartitionSpec, SortOrder, TableMetadataBuilder};

        let schema = iceberg::arrow::arrow_schema_to_schema(&build_arrow_schema()).unwrap();
        let properties = properties
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory:///warehouse/nyc_taxi_database/nyc_taxi_table".to_string(),
            FormatVersion::V2,
            properties,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        Table::builder()
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["nyc_taxi_database", "nyc_taxi_table"]).unwrap())
            .metadata_location(
                "memory:///warehouse/nyc_taxi_database/nyc_taxi_table/metadata/v1.metadata.json",
            )
            .file_io(FileIO::new_with_memory())
            .runtime(iceberg::Runtime::current())
            .build()
            .unwrap()
    }

    /// Writes the 5,000-row payload to `table`; returns the data file and its column codecs.
    async fn write_payload(table: &Table) -> (DataFile, Vec<parquet::basic::Compression>) {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let batch = build_record_batch(&sample_payload().rows).unwrap();
        let mut files = write_data_files(table, batch).await.unwrap();
        assert_eq!(files.len(), 1);
        let file = files.remove(0);
        assert_eq!(file.record_count(), 5000);

        let bytes = table
            .file_io()
            .new_input(file.file_path())
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = SerializedFileReader::new(bytes).unwrap();
        let codecs = reader
            .metadata()
            .row_groups()
            .iter()
            .flat_map(|rg| rg.columns().iter().map(|c| c.compression()))
            .collect();
        (file, codecs)
    }

    #[tokio::test]
    async fn test_data_files_are_zstd_by_default_and_follow_the_table_properties() {
        use parquet::basic::Compression;

        let (zstd_file, codecs) = write_payload(&memory_table(&[])).await;
        assert_eq!(codecs.len(), 23, "one column chunk per column");
        assert!(
            codecs.iter().all(|c| matches!(c, Compression::ZSTD(_))),
            "{codecs:?}"
        );

        let (plain_file, codecs) = write_payload(&memory_table(&[(
            "write.parquet.compression-codec",
            "uncompressed",
        )]))
        .await;
        assert!(
            codecs.iter().all(|c| *c == Compression::UNCOMPRESSED),
            "{codecs:?}"
        );
        // zstd should at least shrink the payload by a quarter (about 55% smaller in practice).
        assert!(
            zstd_file.file_size_in_bytes() * 4 < plain_file.file_size_in_bytes() * 3,
            "zstd {} bytes, uncompressed {} bytes",
            zstd_file.file_size_in_bytes(),
            plain_file.file_size_in_bytes()
        );

        let (_, codecs) = write_payload(&memory_table(&[
            ("write.parquet.compression-codec", "SNAPPY"),
            ("write.parquet.compression-level", "1"),
        ]))
        .await;
        assert!(
            codecs.iter().all(|c| *c == Compression::SNAPPY),
            "{codecs:?}"
        );
    }

    #[tokio::test]
    async fn test_invalid_compression_properties_fail_before_writing() {
        let batch = || build_record_batch(&[sample_row()]).unwrap();
        for props in [
            [("write.parquet.compression-codec", "lzma")],
            [("write.parquet.compression-level", "99")],
        ] {
            let err = write_data_files(&memory_table(&props), batch())
                .await
                .unwrap_err();
            assert!(matches!(err, AppError::Catalog(_)), "{props:?}: {err}");
        }
    }

    #[test]
    fn test_build_record_batch_from_sample_payload() {
        let req = sample_payload();
        let batch = build_record_batch(&req.rows).unwrap();

        assert_eq!(batch.num_rows(), 5000);
        assert_eq!(batch.schema(), build_arrow_schema());
    }
}
