import React, {PropTypes, Component} from 'react'
import {formatRPDuration} from 'utils/formatting'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'
import onClickOutside from 'react-onclickoutside'

class DatabaseRow extends Component {
  constructor(props) {
    super(props)
    this.handleKeyDown = ::this.handleKeyDown
  }

  handleKeyDown(e, db) {
    const {key} = e

    if (rp.isNew) {
      if (key === 'Escape') {
        // return actions.removeRetentionPolicy(db, rp)
      }

      if (key === 'Enter') {
        // return actions.createRetentionPolicyAsync(db, rp)
      }
    }

    if (key === 'Escape') {
      // actions.stopEditRetentionPolicy(db, rp)
    }

    if (key === 'Enter') {
      // actions.updateRetentionPolicy(db, rp)
    }
  }

  render() {
    const {
      retentionPolicy,
      retentionPolicy: {name, duration, replication, isEditing, isDefault, isNew},
      database,
      onEdit,
      onStopEdit,
      onKeyDown,
      onCancel,
      onConfirm,
    } = this.props

    if (isEditing) {
      return (
        <tr>
          <td>
            <div className="admin-table--edit-cell">
              <input
                className="form-control"
                name="name"
                type="text"
                defaultValue={name}
                placeholder="give it a name"
                onKeyDown={(e) => onKeyDown(e, database)}
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
                onKeyDown={(e) => onKeyDown(e, database)}
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
                value={replication || 1}
                placeholder="how many nodes do you have"
                onKeyDown={(e) => onKeyDown(e, database)}
              />
            </div>
          </td>
          <td className="text-right">
            <ConfirmButtons item={{database, retentionPolicy}} onConfirm={isNew ? onConfirm : () => {}} onCancel={isNew ? onCancel : onStopEdit} />
          </td>
        </tr>
      )
    }

    return (
      <tr>
        <td onClick={() => onEdit(database, retentionPolicy)}>
          {name}
          {isDefault ? <span className="default-source-label">default</span> : null}
        </td>
        <td onClick={() => onEdit(database, retentionPolicy)}>{formatRPDuration(duration)}</td>
        <td onClick={() => onEdit(database, retentionPolicy)}>{replication}</td>
        <td className="text-right">
          <button className="btn btn-xs btn-danger admin-table--delete">
            {`Delete ${name}`}
          </button>
        </td>
      </tr>
    )
  }
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
  onStopEdit: func,
  onKeyDown: func,
  onCancel: func,
  onConfirm: func,
}

export default onClickOutside(DatabaseRow)
