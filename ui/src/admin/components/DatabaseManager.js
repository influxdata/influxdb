import React, {PropTypes} from 'react'

import DatabaseTable from 'src/admin/components/DatabaseTable'

const DatabaseManager = ({
  databases,
  notify,
  isRFDisplayed,
  isAddDBDisabled,
  addDatabase,
  onEditDatabase,
  onKeyDownDatabase,
  onCancelDatabase,
  onConfirmDatabase,
  onDeleteDatabase,
  onStartDeleteDatabase,
  onDatabaseDeleteConfirm,
  onRemoveDeleteCode,
  onAddRetentionPolicy,
  onStopEditRetentionPolicy,
  onCancelRetentionPolicy,
  onCreateRetentionPolicy,
  onUpdateRetentionPolicy,
  onRemoveRetentionPolicy,
  onDeleteRetentionPolicy,
}) => {
  return (
    <div className="panel panel-default">
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <h2 className="panel-title">
          {databases.length === 1
            ? '1 Database'
            : `${databases.length} Databases`}
        </h2>
        <button
          className="btn btn-sm btn-primary"
          disabled={isAddDBDisabled}
          onClick={addDatabase}
        >
          Create Database
        </button>
      </div>
      <div className="panel-body">
        {databases.map(db =>
          <DatabaseTable
            key={db.links.self}
            database={db}
            notify={notify}
            isRFDisplayed={isRFDisplayed}
            onEditDatabase={onEditDatabase}
            onKeyDownDatabase={onKeyDownDatabase}
            onCancelDatabase={onCancelDatabase}
            onConfirmDatabase={onConfirmDatabase}
            onRemoveDeleteCode={onRemoveDeleteCode}
            onDeleteDatabase={onDeleteDatabase}
            onStartDeleteDatabase={onStartDeleteDatabase}
            onDatabaseDeleteConfirm={onDatabaseDeleteConfirm}
            onAddRetentionPolicy={onAddRetentionPolicy}
            onStopEditRetentionPolicy={onStopEditRetentionPolicy}
            onCancelRetentionPolicy={onCancelRetentionPolicy}
            onCreateRetentionPolicy={onCreateRetentionPolicy}
            onUpdateRetentionPolicy={onUpdateRetentionPolicy}
            onRemoveRetentionPolicy={onRemoveRetentionPolicy}
            onDeleteRetentionPolicy={onDeleteRetentionPolicy}
          />
        )}
      </div>
    </div>
  )
}

const {arrayOf, bool, func, shape} = PropTypes

DatabaseManager.propTypes = {
  databases: arrayOf(shape()),
  notify: func,
  addDatabase: func,
  isRFDisplayed: bool,
  isAddDBDisabled: bool,
  onEditDatabase: func,
  onKeyDownDatabase: func,
  onCancelDatabase: func,
  onConfirmDatabase: func,
  onDeleteDatabase: func,
  onRemoveDeleteCode: func,
  onStartDeleteDatabase: func,
  onDatabaseDeleteConfirm: func,
  onAddRetentionPolicy: func,
  onEditRetentionPolicy: func,
  onStopEditRetentionPolicy: func,
  onCancelRetentionPolicy: func,
  onCreateRetentionPolicy: func,
  onUpdateRetentionPolicy: func,
  onRemoveRetentionPolicy: func,
  onDeleteRetentionPolicy: func,
}

export default DatabaseManager
