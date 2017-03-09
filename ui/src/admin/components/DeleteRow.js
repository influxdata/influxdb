import React, {PropTypes, Component} from 'react'

import OnClickOutside from 'shared/components/OnClickOutside'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'

const DeleteButton = ({onConfirm}) => (
  <button
    className="btn btn-xs btn-danger admin-table--delete"
    onClick={onConfirm}
  >
    Delete
  </button>
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

  handleClickOutside() {
    this.setState({isConfirmed: false})
  }

  render() {
    const {onDelete, item} = this.props
    const {isConfirmed} = this.state

    if (isConfirmed) {
      return (
        <ConfirmButtons
          onConfirm={onDelete}
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

DeleteRow.propTypes = {
  item: shape({}),
  onDelete: func.isRequired,
}

export default OnClickOutside(DeleteRow)
