use data_types::{
    partition_template::NamespacePartitionTemplateOverride, Column, ColumnId, ColumnSet,
    ColumnType, Namespace, NamespaceId, ObjectStoreId, ParquetFile, ParquetFileId,
    ParquetFileParams, Partition, PartitionId, SkippedCompaction, SortKeyIds, Table, TableId,
    Timestamp,
};
use generated_types::influxdata::iox::catalog::v2 as proto;
use uuid::Uuid;

use crate::interface::SoftDeletedRows;

#[derive(Debug)]
pub struct Error {
    msg: String,
    path: Vec<&'static str>,
}

impl Error {
    fn new<E>(e: E) -> Self
    where
        E: std::fmt::Display,
    {
        Self {
            msg: e.to_string(),
            path: vec![],
        }
    }

    fn ctx(self, arg: &'static str) -> Self {
        let Self { msg, mut path } = self;
        path.insert(0, arg);
        Self { msg, path }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.path.is_empty() {
            write!(f, "{}", self.path[0])?;
            for p in self.path.iter().skip(1) {
                write!(f, ".{}", p)?;
            }
            write!(f, ": ")?;
        }

        write!(f, "{}", self.msg)?;

        Ok(())
    }
}

impl std::error::Error for Error {}

impl From<Error> for crate::interface::Error {
    fn from(e: Error) -> Self {
        Self::External { source: e.into() }
    }
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        Self::invalid_argument(e.to_string())
    }
}

pub(crate) trait ConvertExt<O> {
    fn convert(self) -> Result<O, Error>;
}

impl<T, O> ConvertExt<O> for T
where
    T: TryInto<O>,
    T::Error: std::fmt::Display,
{
    fn convert(self) -> Result<O, Error> {
        self.try_into().map_err(Error::new)
    }
}

pub(crate) trait ConvertOptExt<O> {
    fn convert_opt(self) -> Result<O, Error>;
}

impl<T, O> ConvertOptExt<Option<O>> for Option<T>
where
    T: TryInto<O>,
    T::Error: std::fmt::Display,
{
    fn convert_opt(self) -> Result<Option<O>, Error> {
        self.map(|x| x.convert()).transpose()
    }
}

pub(crate) trait RequiredExt<T> {
    fn required(self) -> Result<T, Error>;
}

impl<T> RequiredExt<T> for Option<T> {
    fn required(self) -> Result<T, Error> {
        self.ok_or_else(|| Error::new("required"))
    }
}

pub(crate) trait ContextExt<T> {
    fn ctx(self, path: &'static str) -> Result<T, Error>;
}

impl<T> ContextExt<T> for Result<T, Error> {
    fn ctx(self, path: &'static str) -> Self {
        self.map_err(|e| e.ctx(path))
    }
}

pub(crate) fn catalog_error_to_status(e: crate::interface::Error) -> tonic::Status {
    use crate::interface::Error;

    match e {
        Error::External { source } => tonic::Status::internal(source.to_string()),
        Error::AlreadyExists { descr } => tonic::Status::already_exists(descr),
        Error::LimitExceeded { descr } => tonic::Status::resource_exhausted(descr),
        Error::NotFound { descr } => tonic::Status::not_found(descr),
    }
}

pub(crate) fn convert_status(status: tonic::Status) -> crate::interface::Error {
    use crate::interface::Error;

    match status.code() {
        tonic::Code::Internal => Error::External {
            source: status.message().to_owned().into(),
        },
        tonic::Code::AlreadyExists => Error::AlreadyExists {
            descr: status.message().to_owned(),
        },
        tonic::Code::ResourceExhausted => Error::LimitExceeded {
            descr: status.message().to_owned(),
        },
        tonic::Code::NotFound => Error::NotFound {
            descr: status.message().to_owned(),
        },
        _ => Error::External {
            source: Box::new(status),
        },
    }
}

pub(crate) fn serialize_soft_deleted_rows(sdr: SoftDeletedRows) -> i32 {
    let sdr = match sdr {
        SoftDeletedRows::AllRows => proto::SoftDeletedRows::AllRows,
        SoftDeletedRows::ExcludeDeleted => proto::SoftDeletedRows::ExcludeDeleted,
        SoftDeletedRows::OnlyDeleted => proto::SoftDeletedRows::OnlyDeleted,
    };

    sdr.into()
}

pub(crate) fn deserialize_soft_deleted_rows(sdr: i32) -> Result<SoftDeletedRows, Error> {
    let sdr: proto::SoftDeletedRows = sdr.convert().ctx("soft deleted rows")?;
    let sdr = match sdr {
        proto::SoftDeletedRows::Unspecified => {
            return Err(Error::new("unspecified soft deleted rows"));
        }
        proto::SoftDeletedRows::AllRows => SoftDeletedRows::AllRows,
        proto::SoftDeletedRows::ExcludeDeleted => SoftDeletedRows::ExcludeDeleted,
        proto::SoftDeletedRows::OnlyDeleted => SoftDeletedRows::OnlyDeleted,
    };
    Ok(sdr)
}

pub(crate) fn serialize_namespace(ns: Namespace) -> proto::Namespace {
    proto::Namespace {
        id: ns.id.get(),
        name: ns.name,
        retention_period_ns: ns.retention_period_ns,
        max_tables: ns.max_tables.get_i32(),
        max_columns_per_table: ns.max_columns_per_table.get_i32(),
        deleted_at: ns.deleted_at.map(|ts| ts.get()),
        partition_template: ns.partition_template.as_proto().cloned(),
    }
}

pub(crate) fn deserialize_namespace(ns: proto::Namespace) -> Result<Namespace, Error> {
    Ok(Namespace {
        id: NamespaceId::new(ns.id),
        name: ns.name,
        retention_period_ns: ns.retention_period_ns,
        max_tables: ns.max_tables.convert().ctx("max_tables")?,
        max_columns_per_table: ns
            .max_columns_per_table
            .convert()
            .ctx("max_columns_per_table")?,
        deleted_at: ns.deleted_at.map(Timestamp::new),
        partition_template: ns
            .partition_template
            .convert_opt()
            .ctx("partition_template")?
            .unwrap_or_else(NamespacePartitionTemplateOverride::const_default),
    })
}

pub(crate) fn serialize_table(t: Table) -> proto::Table {
    proto::Table {
        id: t.id.get(),
        namespace_id: t.namespace_id.get(),
        name: t.name,
        partition_template: t.partition_template.as_proto().cloned(),
    }
}

pub(crate) fn deserialize_table(t: proto::Table) -> Result<Table, Error> {
    Ok(Table {
        id: TableId::new(t.id),
        namespace_id: NamespaceId::new(t.namespace_id),
        name: t.name,
        partition_template: t.partition_template.convert().ctx("partition_template")?,
    })
}

pub(crate) fn serialize_column_type(t: ColumnType) -> i32 {
    use generated_types::influxdata::iox::column_type::v1 as proto;
    proto::ColumnType::from(t).into()
}

pub(crate) fn deserialize_column_type(t: i32) -> Result<ColumnType, Error> {
    use generated_types::influxdata::iox::column_type::v1 as proto;
    let t: proto::ColumnType = t.convert()?;
    t.convert()
}

pub(crate) fn serialize_column(column: Column) -> proto::Column {
    proto::Column {
        id: column.id.get(),
        table_id: column.table_id.get(),
        name: column.name,
        column_type: serialize_column_type(column.column_type),
    }
}

pub(crate) fn deserialize_column(column: proto::Column) -> Result<Column, Error> {
    Ok(Column {
        id: ColumnId::new(column.id),
        table_id: TableId::new(column.table_id),
        name: column.name,
        column_type: deserialize_column_type(column.column_type)?,
    })
}

pub(crate) fn serialize_sort_key_ids(sort_key_ids: &SortKeyIds) -> proto::SortKeyIds {
    proto::SortKeyIds {
        column_ids: sort_key_ids.iter().map(|c_id| c_id.get()).collect(),
    }
}

pub(crate) fn deserialize_sort_key_ids(sort_key_ids: proto::SortKeyIds) -> SortKeyIds {
    SortKeyIds::new(sort_key_ids.column_ids.into_iter().map(ColumnId::new))
}

pub(crate) fn serialize_partition(partition: Partition) -> proto::Partition {
    let empty_sk = SortKeyIds::new(std::iter::empty());

    proto::Partition {
        id: partition.id.get(),
        hash_id: partition
            .hash_id()
            .map(|id| id.as_bytes().to_vec())
            .unwrap_or_default(),
        partition_key: partition.partition_key.inner().to_owned(),
        table_id: partition.table_id.get(),
        sort_key_ids: Some(serialize_sort_key_ids(
            partition.sort_key_ids().unwrap_or(&empty_sk),
        )),
        new_file_at: partition.new_file_at.map(|ts| ts.get()),
    }
}

pub(crate) fn deserialize_partition(partition: proto::Partition) -> Result<Partition, Error> {
    Ok(Partition::new_catalog_only(
        PartitionId::new(partition.id),
        (!partition.hash_id.is_empty())
            .then_some(partition.hash_id.as_slice())
            .convert_opt()
            .ctx("hash_id")?,
        TableId::new(partition.table_id),
        partition.partition_key.into(),
        deserialize_sort_key_ids(partition.sort_key_ids.required().ctx("sort_key_ids")?),
        partition.new_file_at.map(Timestamp::new),
    ))
}

pub(crate) fn serialize_skipped_compaction(sc: SkippedCompaction) -> proto::SkippedCompaction {
    proto::SkippedCompaction {
        partition_id: sc.partition_id.get(),
        reason: sc.reason,
        skipped_at: sc.skipped_at.get(),
        estimated_bytes: sc.estimated_bytes,
        limit_bytes: sc.limit_bytes,
        num_files: sc.num_files,
        limit_num_files: sc.limit_num_files,
        limit_num_files_first_in_partition: sc.limit_num_files_first_in_partition,
    }
}

pub(crate) fn deserialize_skipped_compaction(sc: proto::SkippedCompaction) -> SkippedCompaction {
    SkippedCompaction {
        partition_id: PartitionId::new(sc.partition_id),
        reason: sc.reason,
        skipped_at: Timestamp::new(sc.skipped_at),
        estimated_bytes: sc.estimated_bytes,
        limit_bytes: sc.limit_bytes,
        num_files: sc.num_files,
        limit_num_files: sc.limit_num_files,
        limit_num_files_first_in_partition: sc.limit_num_files_first_in_partition,
    }
}

pub(crate) fn serialize_object_store_id(id: ObjectStoreId) -> proto::ObjectStoreId {
    let (high64, low64) = id.get_uuid().as_u64_pair();
    proto::ObjectStoreId { high64, low64 }
}

pub(crate) fn deserialize_object_store_id(id: proto::ObjectStoreId) -> ObjectStoreId {
    ObjectStoreId::from_uuid(Uuid::from_u64_pair(id.high64, id.low64))
}

pub(crate) fn serialize_column_set(set: &ColumnSet) -> proto::ColumnSet {
    proto::ColumnSet {
        column_ids: set.iter().map(|id| id.get()).collect(),
    }
}

pub(crate) fn deserialize_column_set(set: proto::ColumnSet) -> ColumnSet {
    ColumnSet::new(set.column_ids.into_iter().map(ColumnId::new))
}

pub(crate) fn serialize_parquet_file_params(
    params: &ParquetFileParams,
) -> proto::ParquetFileParams {
    proto::ParquetFileParams {
        namespace_id: params.namespace_id.get(),
        table_id: params.table_id.get(),
        partition_id: params.partition_id.get(),
        partition_hash_id: params
            .partition_hash_id
            .as_ref()
            .map(|id| id.as_bytes().to_vec()),
        object_store_id: Some(serialize_object_store_id(params.object_store_id)),
        min_time: params.min_time.get(),
        max_time: params.max_time.get(),
        file_size_bytes: params.file_size_bytes,
        row_count: params.row_count,
        compaction_level: params.compaction_level as i32,
        created_at: params.created_at.get(),
        column_set: Some(serialize_column_set(&params.column_set)),
        max_l0_created_at: params.max_l0_created_at.get(),
    }
}

pub(crate) fn deserialize_parquet_file_params(
    params: proto::ParquetFileParams,
) -> Result<ParquetFileParams, Error> {
    Ok(ParquetFileParams {
        namespace_id: NamespaceId::new(params.namespace_id),
        table_id: TableId::new(params.table_id),
        partition_id: PartitionId::new(params.partition_id),
        partition_hash_id: params
            .partition_hash_id
            .as_deref()
            .convert_opt()
            .ctx("partition_hash_id")?,
        object_store_id: deserialize_object_store_id(
            params.object_store_id.required().ctx("object_store_id")?,
        ),
        min_time: Timestamp::new(params.min_time),
        max_time: Timestamp::new(params.max_time),
        file_size_bytes: params.file_size_bytes,
        row_count: params.row_count,
        compaction_level: params.compaction_level.convert().ctx("compaction_level")?,
        created_at: Timestamp::new(params.created_at),
        column_set: deserialize_column_set(params.column_set.required().ctx("column_set")?),
        max_l0_created_at: Timestamp::new(params.max_l0_created_at),
    })
}

pub(crate) fn serialize_parquet_file(file: ParquetFile) -> proto::ParquetFile {
    let partition_hash_id = file
        .partition_hash_id
        .map(|x| x.as_bytes().to_vec())
        .unwrap_or_default();

    proto::ParquetFile {
        id: file.id.get(),
        namespace_id: file.namespace_id.get(),
        table_id: file.table_id.get(),
        partition_id: file.partition_id.get(),
        partition_hash_id,
        object_store_id: Some(serialize_object_store_id(file.object_store_id)),
        min_time: file.min_time.get(),
        max_time: file.max_time.get(),
        to_delete: file.to_delete.map(|ts| ts.get()),
        file_size_bytes: file.file_size_bytes,
        row_count: file.row_count,
        compaction_level: file.compaction_level as i32,
        created_at: file.created_at.get(),
        column_set: Some(serialize_column_set(&file.column_set)),
        max_l0_created_at: file.max_l0_created_at.get(),
    }
}

pub(crate) fn deserialize_parquet_file(file: proto::ParquetFile) -> Result<ParquetFile, Error> {
    let partition_hash_id = match file.partition_hash_id.as_slice() {
        b"" => None,
        s => Some(s.convert().ctx("partition_hash_id")?),
    };

    Ok(ParquetFile {
        id: ParquetFileId::new(file.id),
        namespace_id: NamespaceId::new(file.namespace_id),
        table_id: TableId::new(file.table_id),
        partition_id: PartitionId::new(file.partition_id),
        partition_hash_id,
        object_store_id: deserialize_object_store_id(
            file.object_store_id.required().ctx("object_store_id")?,
        ),
        min_time: Timestamp::new(file.min_time),
        max_time: Timestamp::new(file.max_time),
        to_delete: file.to_delete.map(Timestamp::new),
        file_size_bytes: file.file_size_bytes,
        row_count: file.row_count,
        compaction_level: file.compaction_level.convert().ctx("compaction_level")?,
        created_at: Timestamp::new(file.created_at),
        column_set: deserialize_column_set(file.column_set.required().ctx("column_set")?),
        max_l0_created_at: Timestamp::new(file.max_l0_created_at),
    })
}

#[cfg(test)]
mod tests {
    use data_types::{
        partition_template::TablePartitionTemplateOverride, CompactionLevel, PartitionHashId,
        PartitionKey,
    };

    use super::*;

    #[test]
    fn test_column_type_roundtrip() {
        assert_column_type_roundtrip(ColumnType::Bool);
        assert_column_type_roundtrip(ColumnType::I64);
        assert_column_type_roundtrip(ColumnType::U64);
        assert_column_type_roundtrip(ColumnType::F64);
        assert_column_type_roundtrip(ColumnType::String);
        assert_column_type_roundtrip(ColumnType::Tag);
        assert_column_type_roundtrip(ColumnType::Time);
    }

    #[track_caller]
    fn assert_column_type_roundtrip(t: ColumnType) {
        let protobuf = serialize_column_type(t);
        let t2 = deserialize_column_type(protobuf).unwrap();
        assert_eq!(t, t2);
    }

    #[test]
    fn test_error_roundtrip() {
        use crate::interface::Error;

        assert_error_roundtrip(Error::AlreadyExists {
            descr: "foo".to_owned(),
        });
        assert_error_roundtrip(Error::External {
            source: "foo".to_owned().into(),
        });
        assert_error_roundtrip(Error::LimitExceeded {
            descr: "foo".to_owned(),
        });
        assert_error_roundtrip(Error::NotFound {
            descr: "foo".to_owned(),
        });
    }

    #[track_caller]
    fn assert_error_roundtrip(e: crate::interface::Error) {
        let msg_orig = e.to_string();

        let status = catalog_error_to_status(e);
        let e = convert_status(status);
        let msg = e.to_string();
        assert_eq!(msg, msg_orig);
    }

    #[test]
    fn test_soft_deleted_rows_roundtrip() {
        assert_soft_deleted_rows_roundtrip(SoftDeletedRows::AllRows);
        assert_soft_deleted_rows_roundtrip(SoftDeletedRows::ExcludeDeleted);
        assert_soft_deleted_rows_roundtrip(SoftDeletedRows::OnlyDeleted);
    }

    #[track_caller]
    fn assert_soft_deleted_rows_roundtrip(sdr: SoftDeletedRows) {
        let protobuf = serialize_soft_deleted_rows(sdr);
        let sdr2 = deserialize_soft_deleted_rows(protobuf).unwrap();
        assert_eq!(sdr, sdr2);
    }

    #[test]
    fn test_namespace_roundtrip() {
        use generated_types::influxdata::iox::partition_template::v1 as proto;

        let ns = Namespace {
            id: NamespaceId::new(1),
            name: "ns".to_owned(),
            retention_period_ns: Some(2),
            max_tables: 3.try_into().unwrap(),
            max_columns_per_table: 4.try_into().unwrap(),
            deleted_at: Some(Timestamp::new(5)),
            partition_template: NamespacePartitionTemplateOverride::try_from(
                proto::PartitionTemplate {
                    parts: vec![proto::TemplatePart {
                        part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                    }],
                },
            )
            .unwrap(),
        };
        let protobuf = serialize_namespace(ns.clone());
        let ns2 = deserialize_namespace(protobuf).unwrap();
        assert_eq!(ns, ns2);
    }

    #[test]
    fn test_table_roundtrip() {
        use generated_types::influxdata::iox::partition_template::v1 as proto;

        let table = Table {
            id: TableId::new(1),
            namespace_id: NamespaceId::new(2),
            name: "table".to_owned(),
            partition_template: TablePartitionTemplateOverride::try_new(
                Some(proto::PartitionTemplate {
                    parts: vec![proto::TemplatePart {
                        part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                    }],
                }),
                &NamespacePartitionTemplateOverride::const_default(),
            )
            .unwrap(),
        };
        let protobuf = serialize_table(table.clone());
        let table2 = deserialize_table(protobuf).unwrap();
        assert_eq!(table, table2);
    }

    #[test]
    fn test_column_roundtrip() {
        let column = Column {
            id: ColumnId::new(1),
            table_id: TableId::new(2),
            name: "col".to_owned(),
            column_type: ColumnType::F64,
        };
        let protobuf = serialize_column(column.clone());
        let column2 = deserialize_column(protobuf).unwrap();
        assert_eq!(column, column2);
    }

    #[test]
    fn test_sort_key_ids_roundtrip() {
        assert_sort_key_ids_roundtrip(SortKeyIds::new(std::iter::empty()));
        assert_sort_key_ids_roundtrip(SortKeyIds::new([ColumnId::new(1)]));
        assert_sort_key_ids_roundtrip(SortKeyIds::new([
            ColumnId::new(1),
            ColumnId::new(5),
            ColumnId::new(20),
        ]));
    }

    #[track_caller]
    fn assert_sort_key_ids_roundtrip(sort_key_ids: SortKeyIds) {
        let protobuf = serialize_sort_key_ids(&sort_key_ids);
        let sort_key_ids2 = deserialize_sort_key_ids(protobuf);
        assert_eq!(sort_key_ids, sort_key_ids2);
    }

    #[test]
    fn test_partition_roundtrip() {
        let table_id = TableId::new(1);
        let partition_key = PartitionKey::from("key");
        let hash_id = PartitionHashId::new(table_id, &partition_key);

        assert_partition_roundtrip(Partition::new_catalog_only(
            PartitionId::new(2),
            Some(hash_id.clone()),
            table_id,
            partition_key.clone(),
            SortKeyIds::new([ColumnId::new(3), ColumnId::new(4)]),
            Some(Timestamp::new(5)),
        ));
        assert_partition_roundtrip(Partition::new_catalog_only(
            PartitionId::new(2),
            Some(hash_id),
            table_id,
            partition_key,
            SortKeyIds::new(std::iter::empty()),
            Some(Timestamp::new(5)),
        ));
    }

    #[track_caller]
    fn assert_partition_roundtrip(partition: Partition) {
        let protobuf = serialize_partition(partition.clone());
        let partition2 = deserialize_partition(protobuf).unwrap();
        assert_eq!(partition, partition2);
    }

    #[test]
    fn test_skipped_compaction_roundtrip() {
        let sc = SkippedCompaction {
            partition_id: PartitionId::new(1),
            reason: "foo".to_owned(),
            skipped_at: Timestamp::new(2),
            estimated_bytes: 3,
            limit_bytes: 4,
            num_files: 5,
            limit_num_files: 6,
            limit_num_files_first_in_partition: 7,
        };
        let protobuf = serialize_skipped_compaction(sc.clone());
        let sc2 = deserialize_skipped_compaction(protobuf);
        assert_eq!(sc, sc2);
    }

    #[test]
    fn test_object_store_id_roundtrip() {
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::nil()));
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::from_u128(0)));
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::from_u128(u128::MAX)));
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::from_u128(1)));
        assert_object_store_id_roundtrip(ObjectStoreId::from_uuid(Uuid::from_u128(u128::MAX - 1)));
    }

    #[track_caller]
    fn assert_object_store_id_roundtrip(id: ObjectStoreId) {
        let protobuf = serialize_object_store_id(id);
        let id2 = deserialize_object_store_id(protobuf);
        assert_eq!(id, id2);
    }

    #[test]
    fn test_column_set_roundtrip() {
        assert_column_set_roundtrip(ColumnSet::new([]));
        assert_column_set_roundtrip(ColumnSet::new([ColumnId::new(1)]));
        assert_column_set_roundtrip(ColumnSet::new([ColumnId::new(1), ColumnId::new(10)]));
        assert_column_set_roundtrip(ColumnSet::new([
            ColumnId::new(3),
            ColumnId::new(4),
            ColumnId::new(10),
        ]));
    }

    #[track_caller]
    fn assert_column_set_roundtrip(set: ColumnSet) {
        let protobuf = serialize_column_set(&set);
        let set2 = deserialize_column_set(protobuf);
        assert_eq!(set, set2);
    }

    #[test]
    fn test_parquet_file_params_roundtrip() {
        let params = ParquetFileParams {
            namespace_id: NamespaceId::new(1),
            table_id: TableId::new(2),
            partition_id: PartitionId::new(3),
            partition_hash_id: Some(PartitionHashId::arbitrary_for_testing()),
            object_store_id: ObjectStoreId::from_uuid(Uuid::from_u128(1337)),
            min_time: Timestamp::new(4),
            max_time: Timestamp::new(5),
            file_size_bytes: 6,
            row_count: 7,
            compaction_level: CompactionLevel::Final,
            created_at: Timestamp::new(8),
            column_set: ColumnSet::new([ColumnId::new(9), ColumnId::new(10)]),
            max_l0_created_at: Timestamp::new(11),
        };
        let protobuf = serialize_parquet_file_params(&params);
        let params2 = deserialize_parquet_file_params(protobuf).unwrap();
        assert_eq!(params, params2);
    }

    #[test]
    fn test_parquet_file_roundtrip() {
        let file = ParquetFile {
            id: ParquetFileId::new(12),
            namespace_id: NamespaceId::new(1),
            table_id: TableId::new(2),
            partition_id: PartitionId::new(3),
            partition_hash_id: Some(PartitionHashId::arbitrary_for_testing()),
            object_store_id: ObjectStoreId::from_uuid(Uuid::from_u128(1337)),
            min_time: Timestamp::new(4),
            max_time: Timestamp::new(5),
            to_delete: Some(Timestamp::new(13)),
            file_size_bytes: 6,
            row_count: 7,
            compaction_level: CompactionLevel::Final,
            created_at: Timestamp::new(8),
            column_set: ColumnSet::new([ColumnId::new(9), ColumnId::new(10)]),
            max_l0_created_at: Timestamp::new(11),
        };
        let protobuf = serialize_parquet_file(file.clone());
        let file2 = deserialize_parquet_file(protobuf).unwrap();
        assert_eq!(file, file2);
    }
}
