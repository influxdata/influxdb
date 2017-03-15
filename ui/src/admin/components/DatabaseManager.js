import React, {PropTypes} from 'react'
import DatabaseTable from 'src/admin/components/DatabaseTable'

const DatabaseManager = ({
  databases,
  retentionPolicies,
  addDatabase,
  onEditDatabase,
  onKeyDownDatabase,
  onCancelDatabase,
  onConfirmDatabase,
}) => {
  return (
    <div className="panel panel-info">
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <h2 className="panel-title">{databases.length === 1 ? '1 Database' : `${databases.length} Databases`}</h2>
        <div className="btn btn-sm btn-primary" onClick={addDatabase}>Create Database</div>
      </div>
      <div className="panel-body">
        {
          databases.map((db, i) =>
            <DatabaseTable
              key={db.id}
              database={db}
              retentionPolicies={retentionPolicies[i] || []}
              onEditDatabase={onEditDatabase}
              onKeyDownDatabase={onKeyDownDatabase}
              onCancelDatabase={onCancelDatabase}
              onConfirmDatabase={onConfirmDatabase}
            />
          )
        }
      </div>
    </div>
  )
}

const {
  arrayOf,
  func,
  shape,
} = PropTypes

DatabaseManager.propTypes = {
  databases: arrayOf(shape()),
  retentionPolicies: arrayOf(arrayOf(shape)),
  addDatabase: func,
  onEditDatabase: func,
  onKeyDownDatabase: func,
  onCancelDatabase: func,
  onConfirmDatabase: func,
}

export default DatabaseManager

