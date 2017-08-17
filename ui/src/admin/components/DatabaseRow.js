import React, {PropTypes, Component} from 'react'
import onClickOutside from 'react-onclickoutside'

import {formatRPDuration} from 'utils/formatting'
import YesNoButtons from 'shared/components/YesNoButtons'
import {DATABASE_TABLE} from 'src/admin/constants/tableSizing'

class DatabaseRow extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isEditing: false,
      isDeleting: false,
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
    this.handleEndDelete()
  }

  handleStartEdit = () => {
    this.setState({isEditing: true})
  }

  handleEndEdit = () => {
    this.setState({isEditing: false})
  }

  handleStartDelete = () => {
    this.setState({isDeleting: true})
  }

  handleEndDelete = () => {
    this.setState({isDeleting: false})
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
      notify('error', 'Fields cannot be empty')
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
    const {isEditing, isDeleting} = this.state

    const formattedDuration = formatRPDuration(duration)

    if (isEditing) {
      return (
        <tr>
          <td>
            {isNew
              ? <input
                  className="form-control input-xs"
                  type="text"
                  defaultValue={name}
                  placeholder="Name this RP"
                  onKeyDown={this.handleKeyDown}
                  ref={r => (this.name = r)}
                  autoFocus={true}
                  spellCheck={false}
                  autoComplete={false}
                />
              : name}
          </td>
          <td style={{width: `${DATABASE_TABLE.colDuration}px`}}>
            <input
              className="form-control input-xs"
              name="name"
              type="text"
              defaultValue={formattedDuration}
              placeholder="How long should Data last"
              onKeyDown={this.handleKeyDown}
              ref={r => (this.duration = r)}
              autoFocus={!isNew}
              spellCheck={false}
              autoComplete={false}
            />
          </td>
          {isRFDisplayed
            ? <td style={{width: `${DATABASE_TABLE.colReplication}px`}}>
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
                  autoComplete={false}
                />
              </td>
            : null}
          <td
            className="text-right"
            style={{width: `${DATABASE_TABLE.colDelete}px`}}
          >
            <YesNoButtons
              buttonSize="btn-xs"
              onConfirm={isNew ? this.handleCreate : this.handleUpdate}
              onCancel={isNew ? this.handleRemove : this.handleEndEdit}
            />
          </td>
        </tr>
      )
    }

    return (
      <tr>
        <td>
          {`${name} `}
          {isDefault
            ? <span className="default-source-label">default</span>
            : null}
        </td>
        <td
          onClick={this.handleStartEdit}
          style={{width: `${DATABASE_TABLE.colDuration}px`}}
        >
          {formattedDuration}
        </td>
        {isRFDisplayed
          ? <td
              onClick={this.handleStartEdit}
              style={{width: `${DATABASE_TABLE.colReplication}px`}}
            >
              {replication}
            </td>
          : null}
        <td
          className="text-right"
          style={{width: `${DATABASE_TABLE.colDelete}px`}}
        >
          {isDeleting
            ? <YesNoButtons
                onConfirm={onDelete(database, retentionPolicy)}
                onCancel={this.handleEndDelete}
                buttonSize="btn-xs"
              />
            : <button
                className="btn btn-danger btn-xs table--show-on-row-hover"
                style={isDeletable ? {} : {visibility: 'hidden'}}
                onClick={this.handleStartDelete}
              >
                {`Delete ${name}`}
              </button>}
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
  notify: func,
  isRFDisplayed: bool,
}

export default onClickOutside(DatabaseRow)
