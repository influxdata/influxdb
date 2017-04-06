import React, {PropTypes} from 'react'

import _ from 'lodash'

import DatabaseRow from 'src/admin/components/DatabaseRow'
import DatabaseTableHeader from 'src/admin/components/DatabaseTableHeader'

const {
  func,
  shape,
  bool,
} = PropTypes

const DatabaseTable = ({
  database,
  notify,
  isRFDisplayed,
  onEditDatabase,
  onKeyDownDatabase,
  onCancelDatabase,
  onConfirmDatabase,
  onDeleteDatabase,
  onRemoveDeleteCode,
  onStartDeleteDatabase,
  onDatabaseDeleteConfirm,
  onAddRetentionPolicy,
  onCreateRetentionPolicy,
  onUpdateRetentionPolicy,
  onRemoveRetentionPolicy,
  onDeleteRetentionPolicy,
}) => {
  return (
    <div className="db-manager">
      <DatabaseTableHeader
        database={database}
        notify={notify}
        onEdit={onEditDatabase}
        onCancel={onCancelDatabase}
        onDelete={onDeleteDatabase}
        onConfirm={onConfirmDatabase}
        onKeyDown={onKeyDownDatabase}
        onStartDelete={onStartDeleteDatabase}
        onRemoveDeleteCode={onRemoveDeleteCode}
        onAddRetentionPolicy={onAddRetentionPolicy}
        onDeleteRetentionPolicy={onDeleteRetentionPolicy}
        onDatabaseDeleteConfirm={onDatabaseDeleteConfirm}
        isAddRPDisabled={!!database.retentionPolicies.some(rp => rp.isNew)}
      />
      <div className="db-manager-table">
        <table className="table v-center admin-table">
          <thead>
            <tr>
              <th>Retention Policy</th>
              <th>Duration</th>
              {isRFDisplayed ? <th>Replication Factor</th> : null}
              <th></th>
            </tr>
          </thead>
          <tbody>
            {
              _.sortBy(database.retentionPolicies, ({name}) => name.toLowerCase()).map(rp => {
                return (
                  <DatabaseRow
                    key={rp.links.self}
                    notify={notify}
                    database={database}
                    retentionPolicy={rp}
                    onCreate={onCreateRetentionPolicy}
                    onUpdate={onUpdateRetentionPolicy}
                    onRemove={onRemoveRetentionPolicy}
                    onDelete={onDeleteRetentionPolicy}
                    isRFDisplayed={isRFDisplayed}
                    isDeletable={database.retentionPolicies.length > 1}
                  />
                )
              })
            }
          </tbody>
        </table>
      </div>
    </div>
  )
}

DatabaseTable.propTypes = {
  onEditDatabase: func,
  database: shape(),
  notify: func,
  isRFDisplayed: bool,
  isAddRPDisabled: bool,
  onKeyDownDatabase: func,
  onDeleteDatabase: func,
  onCancelDatabase: func,
  onConfirmDatabase: func,
  onRemoveDeleteCode: func,
  onStartDeleteDatabase: func,
  onDatabaseDeleteConfirm: func,
  onAddRetentionPolicy: func,
  onCancelRetentionPolicy: func,
  onCreateRetentionPolicy: func,
  onUpdateRetentionPolicy: func,
  onRemoveRetentionPolicy: func,
  onDeleteRetentionPolicy: func,
}

export default DatabaseTable
