use std::sync::Arc;

use compacted_data::CompactedDataTable;
use compaction_events::CompactionEventsSysTable;
use file_index::FileIndexTable;
use influxdb3_config::EnterpriseConfig;
use influxdb3_enterprise_compactor::compacted_data::CompactedDataSystemTableView;
use influxdb3_sys_events::SysEventStore;
use iox_system_tables::SystemTableProvider;

use super::AllSystemSchemaTablesProvider;

pub(crate) mod compacted_data;
pub(crate) mod compaction_events;
pub(crate) mod file_index;

pub(crate) const COMPACTED_DATA_TABLE_NAME: &str = "compacted_data";
pub(crate) const FILE_INDEX_TABLE_NAME: &str = "file_index";
pub(crate) const COMPACTION_EVENTS_TABLE_NAME: &str = "compaction_events";

impl AllSystemSchemaTablesProvider {
    pub(crate) fn add_enterprise_tables(
        mut self,
        compacted_data: Option<Arc<dyn CompactedDataSystemTableView>>,
        enterprise_config: Arc<EnterpriseConfig>,
        sys_events_store: Arc<SysEventStore>,
    ) -> Self {
        self.tables.insert(
            FILE_INDEX_TABLE_NAME,
            Arc::new(SystemTableProvider::new(Arc::new(FileIndexTable::new(
                self.buffer.catalog(),
                enterprise_config,
            )))),
        );
        let compacted_data_table = Arc::new(SystemTableProvider::new(Arc::new(
            CompactedDataTable::new(Arc::clone(&self.db_schema), compacted_data),
        )));
        self.tables
            .insert(COMPACTED_DATA_TABLE_NAME, compacted_data_table);
        self.tables.insert(
            COMPACTION_EVENTS_TABLE_NAME,
            Arc::new(SystemTableProvider::new(Arc::new(
                CompactionEventsSysTable::new(Arc::clone(&sys_events_store)),
            ))),
        );
        self
    }
}
