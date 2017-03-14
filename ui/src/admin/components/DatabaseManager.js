import React, {PropTypes} from 'react'
import {formatRPDuration} from 'utils/formatting'
import DatabaseTable from 'src/admin/components/DatabaseTable'

const DatabaseManager = ({databases, retentionPolicies, addDatabase}) => {
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
              key={db.name}
              database={db.name}
              retentionPolicies={retentionPolicies[i] || []}
            />
          )
        }
      </div>
    </div>
  )
}

const DatabaseRow = ({name, duration, replication, isDefault}) => {
  return (
    <tr>
      <td>
        {name}
        {isDefault ? <span className="default-source-label">default</span> : null}
      </td>
      <td>{formatRPDuration(duration)}</td>
      <td>{replication}</td>
      <td className="text-right">
        <button className="btn btn-xs btn-danger admin-table--delete">
          {`Delete ${name}`}
        </button>
      </td>
    </tr>
  )
}

const {
  arrayOf,
  bool,
  func,
  number,
  shape,
  string,
} = PropTypes

DatabaseManager.propTypes = {
  databases: arrayOf(shape()),
  retentionPolicies: arrayOf(arrayOf(shape)),
  addDatabase: func,
}

DatabaseRow.propTypes = {
  name: string,
  duration: string,
  replication: number,
  isDefault: bool,
}

export default DatabaseManager

