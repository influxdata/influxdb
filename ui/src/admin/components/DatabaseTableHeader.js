import React, {PropTypes} from 'react'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'

const DatabaseTableHeader = ({
  database,
  onEdit,
  notify,
  onKeyDown,
  onConfirm,
  onCancel,
  onDelete,
  onStartDelete,
  onRemoveDeleteCode,
  onDatabaseDeleteConfirm,
  onAddRetentionPolicy,
  isAddRPDisabled,
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
      notify={notify}
      database={database}
      onCancel={onRemoveDeleteCode}
      onConfirm={onConfirm}
      onDelete={onDelete}
      onStartDelete={onStartDelete}
      isAddRPDisabled={isAddRPDisabled}
      onAddRetentionPolicy={onAddRetentionPolicy}
      onDatabaseDeleteConfirm={onDatabaseDeleteConfirm}
    />
  )
}

const Header = ({
  notify,
  database,
  onCancel,
  onDelete,
  onStartDelete,
  isAddRPDisabled,
  onAddRetentionPolicy,
  onDatabaseDeleteConfirm,
}) => {
  const confirmStyle = {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
  }

  const buttons = (
    <div className="text-right">
      {
        database.name === '_internal' ? null :
          <button className="btn btn-xs btn-danger" onClick={() => onStartDelete(database)}>
            Delete
          </button>
      }
      <button className="btn btn-xs btn-primary" disabled={isAddRPDisabled} onClick={() => onAddRetentionPolicy(database)}>
        Add retention policy
      </button>
    </div>
  )

  const onConfirm = (db) => {
    if (database.deleteCode !== `DELETE ${database.name}`) {
      return notify('error', `Type DELETE ${database.name} to confirm`)
    }

    onDelete(db)
  }

  const deleteConfirmation = (
    <div style={confirmStyle}>
      <div className="admin-table--delete-cell">
        <input
          className="form-control"
          name="name"
          type="text"
          value={database.deleteCode || ''}
          placeholder={`DELETE ${database.name}`}
          onChange={(e) => onDatabaseDeleteConfirm(database, e)}
          onKeyDown={(e) => onDatabaseDeleteConfirm(database, e)}
          autoFocus={true}
        />
      </div>
      <ConfirmButtons item={database} onConfirm={onConfirm} onCancel={onCancel} />
    </div>
  )

  return (
    <div className="db-manager-header">
      <h4>{database.name}</h4>
      {database.hasOwnProperty('deleteCode') ? deleteConfirmation : buttons}
    </div>
  )
}

const EditHeader = ({database, onEdit, onKeyDown, onConfirm, onCancel}) => (
  <div className="db-manager-header-edit">
    <input
      className="form-control"
      name="name"
      type="text"
      value={database.name}
      placeholder="Database name"
      onChange={(e) => onEdit(database, {name: e.target.value})}
      onKeyDown={(e) => onKeyDown(e, database)}
      autoFocus={true}
    />
    <ConfirmButtons item={database} onConfirm={onConfirm} onCancel={onCancel} />
  </div>
)

const {
  func,
  shape,
  bool,
} = PropTypes

DatabaseTableHeader.propTypes = {
  onEdit: func,
  notify: func,
  database: shape(),
  onKeyDown: func,
  onCancel: func,
  onConfirm: func,
  onDelete: func,
  onStartDelete: func,
  onDatabaseDeleteConfirm: func,
  onRemoveDeleteCode: func,
  onAddRetentionPolicy: func,
  isAddRPDisabled: bool,
}


Header.propTypes = {
  notify: func,
  onConfirm: func,
  onCancel: func,
  onDelete: func,
  database: shape(),
  onStartDelete: func,
  isAddRPDisabled: bool,
  onAddRetentionPolicy: func,
  onDatabaseDeleteConfirm: func,
}

EditHeader.propTypes = {
  database: shape(),
  onEdit: func,
  onKeyDown: func,
  onCancel: func,
  onConfirm: func,
  isRFDisplayed: bool,
}

export default DatabaseTableHeader
