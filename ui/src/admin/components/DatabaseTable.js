import React, {PropTypes} from 'react'

import _ from 'lodash'
import classnames from 'classnames'

import DatabaseRow from 'src/admin/components/DatabaseRow'
import DatabaseTableHeader from 'src/admin/components/DatabaseTableHeader'
import {DATABASE_TABLE} from 'src/admin/constants/tableSizing'

const {func, shape, bool} = PropTypes

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
    <div
      className={classnames('db-manager', {
        'db-manager--edit': database.isEditing,
      })}
    >
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
        <table className="table v-center table-highlight">
          <thead>
            <tr>
              <th>Retention Policy</th>
              <th style={{width: `${DATABASE_TABLE.colDuration}px`}}>
                Duration
              </th>
              {isRFDisplayed
                ? <th style={{width: `${DATABASE_TABLE.colReplication}px`}}>
                    Replication Factor
                  </th>
                : null}
              <th style={{width: `${DATABASE_TABLE.colDelete}px`}} />
            </tr>
          </thead>
          <tbody>
            {_.sortBy(database.retentionPolicies, ({name}) =>
              name.toLowerCase()
            ).map(rp => {
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
            })}
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
