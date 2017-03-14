import React, {PropTypes} from 'react'

const DatabaseManager = ({databases, retentionPolicies}) => {
  return (
    <div className="panel panel-info">
      {databases.length} Databases
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
    <div>
      <h2>{database}</h2>
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
            retentionPolicies.map((rp, i) => {
              return (
                <DatabaseRow
                  key={i}
                  retentionPolicy={rp}
                />
              )
            })
          }
        </tbody>
      </table>
    </div>
  )
}

const DatabaseRow = ({retentionPolicy}) => {
  return (
    <tr>
      <td>{retentionPolicy.name}</td>
    </tr>
  )
}

const {
  arrayOf,
  shape,
  string,
} = PropTypes

DatabaseManager.propTypes = {
  databases: arrayOf(string),
  retentionPolicies: arrayOf(arrayOf(shape)),
}

DatabaseRow.propTypes = {
  retentionPolicy: shape(),
}

DatabaseTable.propTypes = {
  database: string,
  retentionPolicies: arrayOf(shape()),
}

export default DatabaseManager

