import React, {PropTypes} from 'react'
import {formatRPDuration} from 'utils/formatting'

const DatabaseManager = ({databases, retentionPolicies}) => {
  return (
    <div className="panel panel-info">
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <h2 className="panel-title">{databases.length === 1 ? '1 Database' : `${databases.length} Databases`}</h2>
        <div className="btn btn-sm btn-primary">Create Database</div>
      </div>
      <div className="panel-body">
        {
          databases.map((db, i) =>
            <DatabaseTable
              key={i}
              database={db}
              retentionPolicies={retentionPolicies[i] || []}
            />
          )
        }
      </div>
    </div>
  )
}

const DatabaseTable = ({database, retentionPolicies}) => {
  return (
    <div className="db-manager">
      <div className="db-manager-header">
        <h4>{database}</h4>
        <div className="text-right">
          <button className="btn btn-xs btn-danger">
            Delete
          </button>
          <button className="btn btn-xs btn-primary">
            {`Add retention policy`}
          </button>
        </div>
      </div>
      <div className="db-manager-table">
        <table className="table v-center admin-table">
          <thead>
            <tr>
              <th>Retention Policy</th>
              <th>Duration</th>
              <th>Replication Factor</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {
              retentionPolicies.map(({name, duration, replication, isDefault}) => {
                return (
                  <DatabaseRow
                    key={name}
                    name={name}
                    duration={duration}
                    replication={replication}
                    isDefault={isDefault}
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
  number,
  shape,
  string,
} = PropTypes

DatabaseManager.propTypes = {
  databases: arrayOf(string),
  retentionPolicies: arrayOf(arrayOf(shape)),
}

DatabaseRow.propTypes = {
  name: string,
  duration: string,
  replication: number,
  isDefault: bool,
}

DatabaseTable.propTypes = {
  database: string,
  retentionPolicies: arrayOf(shape()),
}

export default DatabaseManager

