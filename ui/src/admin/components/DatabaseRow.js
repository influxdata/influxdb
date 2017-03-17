import React, {PropTypes} from 'react'
import {formatRPDuration} from 'utils/formatting'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'

export const DatabaseRow = ({
  retentionPolicy,
  retentionPolicy: {name, duration, replication, isEditing, isDefault},
  database,
  onEdit,
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
              placeholder="retention policy name"
              onChange={(e) => onEdit(database, {...retentionPolicy, name: e.target.value})}
              onKeyDown={() => {}}
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
              placeholder="duration"
              onChange={(e) => onEdit(database, {...retentionPolicy, duration: e.target.value})}
              onKeyDown={() => {}}
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
              value={replication}
              placeholder="replication factor"
              onChange={(e) => onEdit(database, {...retentionPolicy, replication: +e.target.value})}
              onKeyDown={() => {}}
            />
          </div>
        </td>
        <td className="text-right">
          <ConfirmButtons item={{}} onConfirm={() => {}} onCancel={() => {}} />
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
}
