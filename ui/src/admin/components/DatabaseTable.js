import React, {PropTypes} from 'react'
import {DatabaseRow} from 'src/admin/components/DatabaseRow'

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

const {
  arrayOf,
  shape,
  string,
} = PropTypes

DatabaseTable.propTypes = {
  database: string,
  retentionPolicies: arrayOf(shape()),
}

export default DatabaseTable
