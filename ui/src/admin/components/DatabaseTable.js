import React, {PropTypes} from 'react'
import {DatabaseRow} from 'src/admin/components/DatabaseRow'

const DatabaseTable = ({database, retentionPolicies, onEditDatabase}) => {
  return (
    <div className="db-manager">
      <DatabaseTableHeader database={database} onEdit={onEditDatabase} />
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

const DatabaseTableHeader = ({database, onEdit}) => {
  if (database.isEditing) {
    return <EditHeader database={database} onEdit={onEdit} />
  }

  return <Header database={database} />
}

const Header = ({database}) => (
  <div className="db-manager-header">
    <h4>{database.name}</h4>
    <div className="text-right">
      <button className="btn btn-xs btn-danger">
        Delete
      </button>
      <button className="btn btn-xs btn-primary">
        {`Add retention policy`}
      </button>
    </div>
  </div>
)

const EditHeader = ({database, onEdit, onKeyPress}) => (
  <div className="db-manager-header">
    <h4>
      <div className="admin-table--edit-cell">
        <input
          className="form-control"
          name="name"
          type="text"
          value={database.name}
          placeholder="database name"
          onChange={(e) => onEdit(e.target.value, database)}
          onKeyPress={(e) => onKeyPress(e, database)}
          autoFocus={true}
        />
      </div>
    </h4>
  </div>
)

const {
  arrayOf,
  func,
  shape,
} = PropTypes

DatabaseTable.propTypes = {
  onEditDatabase: func,
  database: shape(),
  retentionPolicies: arrayOf(shape()),
}

Header.propTypes = {
  database: shape(),
}

EditHeader.propTypes = {
  onEdit: func,
  database: shape(),
}

DatabaseTableHeader.propTypes = {
  onEdit: func,
  database: shape(),
}

export default DatabaseTable
