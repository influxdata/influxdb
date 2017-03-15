import React, {PropTypes} from 'react'
import {DatabaseRow} from 'src/admin/components/DatabaseRow'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'

const DatabaseTable = ({
  database,
  retentionPolicies,
  onEditDatabase,
  onKeyDownDatabase,
  onCancelDatabase,
  onConfirmDatabase,
}) => {
  return (
    <div className="db-manager">
      <DatabaseTableHeader
        database={database}
        onEdit={onEditDatabase}
        onKeyDown={onKeyDownDatabase}
        onCancel={onCancelDatabase}
        onConfirm={onConfirmDatabase}
      />
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

const DatabaseTableHeader = ({database, onEdit, onKeyDown, onConfirm, onCancel}) => {
  if (database.isEditing) {
    return (
      <EditHeader
        database={database}
        onEdit={onEdit}
        onKeyDown={onKeyDown}
        onConfirm={onConfirm}
        onCancel={onCancel}
      />
    )
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

const EditHeader = ({database, onEdit, onKeyDown, onConfirm, onCancel}) => (
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
          onKeyDown={(e) => onKeyDown(e, database)}
          autoFocus={true}
        />
      </div>
    </h4>
    <ConfirmButtons item={database} onConfirm={onConfirm} onCancel={onCancel} />
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
  onKeyDownDatabase: func,
  onCancelDatabase: func,
  onConfirmDatabase: func,
}

Header.propTypes = {
  database: shape(),
}

EditHeader.propTypes = {
  database: shape(),
  onEdit: func,
  onKeyDown: func,
  onCancel: func,
  onConfirm: func,
}

DatabaseTableHeader.propTypes = {
  onEdit: func,
  database: shape(),
  onKeyDown: func,
  onCancel: func,
  onConfirm: func,
}

export default DatabaseTable
