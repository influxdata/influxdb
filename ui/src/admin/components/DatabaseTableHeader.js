import React, {PropTypes} from 'react'
import ConfirmButtons from 'shared/components/ConfirmButtons'

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
  const buttons = (
    <div className="text-right db-manager-header--actions">
      <button
        className="btn btn-xs btn-primary"
        disabled={isAddRPDisabled}
        onClick={onAddRetentionPolicy(database)}
      >
        Add Retention Policy
      </button>
      {database.name === '_internal'
        ? null
        : <button
            className="btn btn-xs btn-danger"
            onClick={onStartDelete(database)}
          >
            Delete
          </button>}
    </div>
  )

  const onConfirm = db => {
    if (database.deleteCode !== `DELETE ${database.name}`) {
      return notify('error', `Type DELETE ${database.name} to confirm`)
    }

    onDelete(db)
  }

  const deleteConfirmation = (
    <div className="admin-table--delete-db">
      <input
        className="form-control input-xs"
        name="name"
        type="text"
        value={database.deleteCode || ''}
        placeholder={`DELETE ${database.name}`}
        onChange={onDatabaseDeleteConfirm(database)}
        onKeyDown={onDatabaseDeleteConfirm(database)}
        autoFocus={true}
        autoComplete={false}
        spellCheck={false}
      />
      <ConfirmButtons
        item={database}
        onConfirm={onConfirm}
        onCancel={onCancel}
        buttonSize="btn-xs"
      />
    </div>
  )

  return (
    <div className="db-manager-header">
      <h4>
        {database.name}
      </h4>
      {database.hasOwnProperty('deleteCode') ? deleteConfirmation : buttons}
    </div>
  )
}

const EditHeader = ({database, onEdit, onKeyDown, onConfirm, onCancel}) =>
  <div className="db-manager-header db-manager-header--edit">
    <input
      className="form-control input-sm"
      name="name"
      type="text"
      value={database.name}
      placeholder="Name this Database"
      onChange={onEdit(database)}
      onKeyDown={onKeyDown(database)}
      autoFocus={true}
      spellCheck={false}
      autoComplete={false}
    />
    <ConfirmButtons item={database} onConfirm={onConfirm} onCancel={onCancel} />
  </div>

const {func, shape, bool} = PropTypes

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
