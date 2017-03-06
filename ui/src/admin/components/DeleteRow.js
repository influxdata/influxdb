import React, {PropTypes, Component} from 'react'

const DeleteButton = ({onConfirm}) => (
  <button
    className="btn btn-xs btn-danger admin-table--delete"
    onClick={onConfirm}
  >
    Delete
  </button>
)

const ConfirmButtons = ({onDelete, item, onCancel}) => (
  <div className="admin-table--delete">
    <button
      className="btn btn-xs btn-primary "
      onClick={() => onDelete(item)}
    >
      Confirm
    </button>
    <button
      className="btn btn-xs btn-default"
      onClick={onCancel}
    >
      Cancel
    </button>
  </div>
)

class DeleteRow extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isConfirmed: false,
    }
    this.handleConfirm = ::this.handleConfirm
    this.handleCancel = ::this.handleCancel
  }

  handleConfirm() {
    this.setState({isConfirmed: true})
  }

  handleCancel() {
    this.setState({isConfirmed: false})
  }

  render() {
    const {onDelete, item} = this.props
    const {isConfirmed} = this.state

    if (isConfirmed) {
      return (
        <ConfirmButtons
          onDelete={onDelete}
          item={item}
          onCancel={this.handleCancel}
        />
      )
    }

    return (
      <DeleteButton onConfirm={this.handleConfirm} />
    )
  }
}

const {
  func,
  shape,
} = PropTypes

DeleteButton.propTypes = {
  onConfirm: func.isRequired,
}

ConfirmButtons.propTypes = {
  onDelete: func.isRequired,
  item: shape({}).isRequired,
  onCancel: func.isRequired,
}

DeleteRow.propTypes = {
  item: shape({}),
  onDelete: func.isRequired,
}

export default DeleteRow
