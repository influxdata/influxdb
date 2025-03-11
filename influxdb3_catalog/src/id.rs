use influxdb3_id::{ColumnId, DbId, TableId};

use crate::catalog::{DatabaseSchema, InnerCatalog, TableDefinition};

pub(crate) trait IdProvider {
    type Identifier;

    fn get_next_id(&self) -> Self::Identifier;
    fn get_and_increment_next_id(&mut self) -> Self::Identifier;
}

impl IdProvider for InnerCatalog {
    type Identifier = DbId;

    fn get_and_increment_next_id(&mut self) -> Self::Identifier {
        let next = self.next_db_id;
        self.next_db_id = self.next_db_id.next();
        next
    }

    fn get_next_id(&self) -> Self::Identifier {
        self.next_db_id
    }
}

impl IdProvider for DatabaseSchema {
    type Identifier = TableId;

    fn get_and_increment_next_id(&mut self) -> Self::Identifier {
        let next = self.next_table_id;
        self.next_table_id = self.next_table_id.next();
        next
    }

    fn get_next_id(&self) -> Self::Identifier {
        self.next_table_id
    }
}

impl IdProvider for TableDefinition {
    type Identifier = ColumnId;

    fn get_and_increment_next_id(&mut self) -> Self::Identifier {
        let next = self.next_column_id;
        self.next_column_id = self.next_column_id.next();
        next
    }

    fn get_next_id(&self) -> Self::Identifier {
        self.next_column_id
    }
}
