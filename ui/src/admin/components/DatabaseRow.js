import React, {PropTypes} from 'react'
import {formatRPDuration} from 'utils/formatting'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'

export const DatabaseRow = ({
  retentionPolicy,
  retentionPolicy: {name, duration, replication, isEditing, isDefault},
  database,
  onEdit,
  onKeyDown,
  onCancel,
  onConfirm,
}) => {
  if (isEditing) {
    return (
      <tr>
        <td>
          <div className="admin-table--edit-cell">
            <input
              className="form-control"
              name="name"
              type="text"
              value={name}
              placeholder="give it a name"
              onChange={(e) => onEdit(database, {...retentionPolicy, name: e.target.value})}
              onKeyDown={(e) => onKeyDown(e, database, retentionPolicy)}
              autoFocus={true}
            />
          </div>
        </td>
        <td>
          <div className="admin-table--edit-cell">
            <input
              className="form-control"
              name="name"
              type="text"
              value={duration}
              placeholder="how long should data last"
              onChange={(e) => onEdit(database, {...retentionPolicy, duration: e.target.value})}
              onKeyDown={(e) => onKeyDown(e, database, retentionPolicy)}
            />
          </div>
        </td>
        <td>
          <div className="admin-table--edit-cell">
            <input
              className="form-control"
              name="name"
              type="number"
              min="1"
              value={replication || ''}
              placeholder="how many nodes do you have"
              onChange={(e) => onEdit(database, {...retentionPolicy, replication: +e.target.value})}
              onKeyDown={(e) => onKeyDown(e, database, retentionPolicy)}
            />
          </div>
        </td>
        <td className="text-right">
          <ConfirmButtons item={{database, retentionPolicy}} onConfirm={onConfirm} onCancel={onCancel} />
        </td>
      </tr>
    )
  }

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
  bool,
  func,
  number,
  shape,
  string,
} = PropTypes

DatabaseRow.propTypes = {
  retentionPolicy: shape({
    name: string,
    duration: string,
    replication: number,
    isDefault: bool,
    isEditing: bool,
  }),
  database: shape(),
  onEdit: func,
  onKeyDown: func,
  onCancel: func,
  onConfirm: func,
}
