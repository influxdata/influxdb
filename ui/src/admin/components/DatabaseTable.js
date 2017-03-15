import React, {PropTypes} from 'react'
import {DatabaseRow} from 'src/admin/components/DatabaseRow'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'

const {
  func,
  shape,
} = PropTypes

const DatabaseTable = ({
  database,
  onEditDatabase,
  onKeyDownDatabase,
  onCancelDatabase,
  onConfirmDatabase,
  onStartDeleteDatabase,
  onDatabaseDeleteConfirm,
  onAddRetentionPolicy,
}) => {
  return (
    <div className="db-manager">
      <DatabaseTableHeader
        database={database}
        onEdit={onEditDatabase}
        onKeyDown={onKeyDownDatabase}
        onCancel={onCancelDatabase}
        onConfirm={onConfirmDatabase}
        onStartDelete={onStartDeleteDatabase}
        onDatabaseDeleteConfirm={onDatabaseDeleteConfirm}
        onAddRetentionPolicy={onAddRetentionPolicy}
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
              database.retentionPolicies.map(({name, duration, replication, isDefault}) => {
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

DatabaseTable.propTypes = {
  onEditDatabase: func,
  database: shape(),
  onKeyDownDatabase: func,
  onCancelDatabase: func,
  onConfirmDatabase: func,
  onStartDeleteDatabase: func,
  onDatabaseDeleteConfirm: func,
  onAddRetentionPolicy: func,
}

const DatabaseTableHeader = ({
  database,
  onEdit,
  onKeyDown,
  onConfirm,
  onCancel,
  onStartDelete,
  onDatabaseDeleteConfirm,
  onAddRetentionPolicy,
}) => {
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

  return (
    <Header
      database={database}
      onStartDelete={onStartDelete}
      onDatabaseDeleteConfirm={onDatabaseDeleteConfirm}
      onAddRetentionPolicy={onAddRetentionPolicy}
    />
  )
}

DatabaseTableHeader.propTypes = {
  onEdit: func,
  database: shape(),
  onKeyDown: func,
  onCancel: func,
  onConfirm: func,
  onStartDelete: func,
  onDatabaseDeleteConfirm: func,
  onAddRetentionPolicy: func,
}

const Header = ({
  database,
  onStartDelete,
  onDatabaseDeleteConfirm,
  onAddRetentionPolicy,
}) => {
  const confirmStyle = {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
  }

  const buttons = (
    <div className="text-right">
      <button className="btn btn-xs btn-danger" onClick={() => onStartDelete(database)}>
        Delete
      </button>
      <button className="btn btn-xs btn-primary" onClick={() => onAddRetentionPolicy(database)}>
        Add retention policy
      </button>
    </div>
  )

  const deleteConfirm = (
    <div style={confirmStyle}>
      <div className="admin-table--delete-cell">
        <input
          className="form-control"
          name="name"
          type="text"
          value={database.deleteCode || ''}
          placeholder="type DELETE to confirm"
          onChange={(e) => onDatabaseDeleteConfirm(database, e)}
          onKeyDown={(e) => onDatabaseDeleteConfirm(database, e)}
          autoFocus={true}
        />
      </div>
      <ConfirmButtons item={database} onConfirm={() => {}} onCancel={() => {}} />
    </div>
  )

  return (
    <div className="db-manager-header">
      <h4>{database.name}</h4>
      {database.hasOwnProperty('deleteCode') ? deleteConfirm : buttons}
    </div>
  )
}

Header.propTypes = {
  database: shape(),
  onStartDelete: func,
  onDatabaseDeleteConfirm: func,
  onAddRetentionPolicy: func,
}

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

EditHeader.propTypes = {
  database: shape(),
  onEdit: func,
  onKeyDown: func,
  onCancel: func,
  onConfirm: func,
}

export default DatabaseTable
