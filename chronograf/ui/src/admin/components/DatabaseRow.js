import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import onClickOutside from 'react-onclickoutside'

import {notify as notifyAction} from 'shared/actions/notifications'

import {formatRPDuration} from 'utils/formatting'
import ConfirmButton from 'shared/components/ConfirmButton'
import {DATABASE_TABLE} from 'src/admin/constants/tableSizing'
import {notifyRetentionPolicyCantHaveEmptyFields} from 'shared/copy/notifications'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class DatabaseRow extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isEditing: false,
    }
  }

  componentWillMount() {
    if (this.props.retentionPolicy.isNew) {
      this.setState({isEditing: true})
    }
  }

  handleRemove = () => {
    const {database, retentionPolicy, onRemove} = this.props
    onRemove(database, retentionPolicy)
  }

  handleClickOutside = () => {
    const {database, retentionPolicy, onRemove} = this.props
    if (retentionPolicy.isNew) {
      onRemove(database, retentionPolicy)
    }

    this.handleEndEdit()
  }

  handleStartEdit = () => {
    this.setState({isEditing: true})
  }

  handleEndEdit = () => {
    this.setState({isEditing: false})
  }

  handleCreate = () => {
    const {database, retentionPolicy, onCreate} = this.props
    const validInputs = this.getInputValues()
    if (!validInputs) {
      return
    }

    onCreate(database, {...retentionPolicy, ...validInputs})
    this.handleEndEdit()
  }

  handleUpdate = () => {
    const {database, retentionPolicy, onUpdate} = this.props
    const validInputs = this.getInputValues()
    if (!validInputs) {
      return
    }

    onUpdate(database, retentionPolicy, validInputs)
    this.handleEndEdit()
  }

  handleKeyDown = e => {
    const {key} = e
    const {retentionPolicy, database, onRemove} = this.props

    if (key === 'Escape') {
      if (retentionPolicy.isNew) {
        onRemove(database, retentionPolicy)
        return
      }

      this.handleEndEdit()
    }

    if (key === 'Enter') {
      if (retentionPolicy.isNew) {
        this.handleCreate()
        return
      }

      this.handleUpdate()
    }
  }

  getInputValues = () => {
    const {
      notify,
      retentionPolicy: {name: currentName},
      isRFDisplayed,
    } = this.props

    const name = (this.name && this.name.value.trim()) || currentName
    let duration = this.duration.value.trim()
    // Replication > 1 is only valid for Influx Enterprise
    const replication = isRFDisplayed ? +this.replication.value.trim() : 1

    if (!duration || (isRFDisplayed && !replication)) {
      notify(notifyRetentionPolicyCantHaveEmptyFields())
      return
    }

    if (duration === '∞') {
      duration = 'INF'
    }

    return {
      name,
      duration,
      replication,
    }
  }

  render() {
    const {
      retentionPolicy: {name, duration, replication, isDefault, isNew},
      retentionPolicy,
      database,
      onDelete,
      isDeletable,
      isRFDisplayed,
    } = this.props
    const {isEditing} = this.state

    const formattedDuration = formatRPDuration(duration)

    if (isEditing) {
      return (
        <tr>
          <td style={{width: `${DATABASE_TABLE.colRetentionPolicy}px`}}>
            {isNew ? (
              <input
                className="form-control input-xs"
                type="text"
                defaultValue={name}
                placeholder="Name this RP"
                onKeyDown={this.handleKeyDown}
                ref={r => (this.name = r)}
                autoFocus={true}
                spellCheck={false}
                autoComplete="false"
              />
            ) : (
              name
            )}
          </td>
          <td style={{width: `${DATABASE_TABLE.colDuration}px`}}>
            <input
              className="form-control input-xs"
              name="name"
              type="text"
              defaultValue={formattedDuration}
              placeholder="INF, 1h30m, 1d, etc"
              onKeyDown={this.handleKeyDown}
              ref={r => (this.duration = r)}
              autoFocus={!isNew}
              spellCheck={false}
              autoComplete="false"
            />
          </td>
          {isRFDisplayed ? (
            <td style={{width: `${DATABASE_TABLE.colReplication}px`}}>
              <input
                className="form-control input-xs"
                name="name"
                type="number"
                min="1"
                defaultValue={replication || 1}
                placeholder="# of Nodes"
                onKeyDown={this.handleKeyDown}
                ref={r => (this.replication = r)}
                spellCheck={false}
                autoComplete="false"
              />
            </td>
          ) : null}
          <td
            className="text-right"
            style={{width: `${DATABASE_TABLE.colDelete}px`}}
          >
            <button
              className="btn btn-xs btn-info"
              onClick={isNew ? this.handleRemove : this.handleEndEdit}
            >
              Cancel
            </button>
            <button
              className="btn btn-xs btn-success"
              onClick={isNew ? this.handleCreate : this.handleUpdate}
            >
              Save
            </button>
          </td>
        </tr>
      )
    }

    return (
      <tr>
        <td>
          {`${name} `}
          {isDefault ? (
            <span className="default-source-label">default</span>
          ) : null}
        </td>
        <td style={{width: `${DATABASE_TABLE.colDuration}px`}}>
          {formattedDuration}
        </td>
        {isRFDisplayed ? (
          <td style={{width: `${DATABASE_TABLE.colReplication}px`}}>
            {replication}
          </td>
        ) : null}
        <td
          className="text-right"
          style={{width: `${DATABASE_TABLE.colDelete}px`}}
        >
          <button
            className="btn btn-xs btn-info table--show-on-row-hover"
            onClick={this.handleStartEdit}
          >
            Edit
          </button>
          <ConfirmButton
            text={`Delete ${name}`}
            confirmAction={onDelete(database, retentionPolicy)}
            size="btn-xs"
            type="btn-danger"
            customClass="table--show-on-row-hover"
            disabled={!isDeletable}
          />
        </td>
      </tr>
    )
  }
}

const {bool, func, number, shape, string} = PropTypes

DatabaseRow.propTypes = {
  retentionPolicy: shape({
    name: string,
    duration: string,
    replication: number,
    isDefault: bool,
    isEditing: bool,
  }),
  isDeletable: bool,
  database: shape(),
  onRemove: func,
  onCreate: func,
  onUpdate: func,
  onDelete: func,
  notify: func.isRequired,
  isRFDisplayed: bool,
}

const mapDispatchToProps = dispatch => ({
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(null, mapDispatchToProps)(onClickOutside(DatabaseRow))
