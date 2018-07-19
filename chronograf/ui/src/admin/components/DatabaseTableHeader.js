import React from 'react'
import PropTypes from 'prop-types'

import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {notify as notifyAction} from 'shared/actions/notifications'
import ConfirmOrCancel from 'shared/components/ConfirmOrCancel'
import {notifyDatabaseDeleteConfirmationRequired} from 'shared/copy/notifications'

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
    <div className="db-manager-header--actions text-right">
      <button
        className="btn btn-xs btn-primary"
        disabled={isAddRPDisabled}
        onClick={onAddRetentionPolicy(database)}
      >
        <span className="icon plus" /> Add Retention Policy
      </button>
      {database.name === '_internal' ? null : (
        <button
          className="btn btn-xs btn-danger"
          onClick={onStartDelete(database)}
        >
          Delete
        </button>
      )}
    </div>
  )

  function onConfirm(db) {
    if (database.deleteCode !== `DELETE ${database.name}`) {
      return notify(notifyDatabaseDeleteConfirmationRequired(database.name))
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
        autoComplete="false"
        spellCheck={false}
      />
      <ConfirmOrCancel
        item={database}
        onConfirm={onConfirm}
        onCancel={onCancel}
        buttonSize="btn-xs"
      />
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
      autoComplete="false"
    />
    <ConfirmOrCancel
      item={database}
      onConfirm={onConfirm}
      onCancel={onCancel}
    />
  </div>
)

const {func, shape, bool} = PropTypes

DatabaseTableHeader.propTypes = {
  onEdit: func,
  notify: func.isRequired,
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
  notify: func.isRequired,
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

const mapDispatchToProps = dispatch => ({
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(null, mapDispatchToProps)(DatabaseTableHeader)
