import React, {PropTypes, Component} from 'react'

import OnClickOutside from 'shared/components/OnClickOutside'
import ConfirmButtons from 'shared/components/ConfirmButtons'

const DeleteButton = ({onClickDelete}) => (
  <button
    className="btn btn-xs btn-danger admin-table--hidden"
    onClick={onClickDelete}
  >
    Delete
  </button>
)

class DeleteConfirmButtons extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isConfirming: false,
    }
    this.handleClickDelete = ::this.handleClickDelete
    this.handleCancel = ::this.handleCancel
  }

  handleClickDelete() {
    this.setState({isConfirming: true})
  }

  handleCancel() {
    this.setState({isConfirming: false})
  }

  handleClickOutside() {
    this.setState({isConfirming: false})
  }

  render() {
    const {onDelete, item} = this.props
    const {isConfirming} = this.state

    return isConfirming
      ? <ConfirmButtons
          onConfirm={onDelete}
          item={item}
          onCancel={this.handleCancel}
        />
      : <DeleteButton onClickDelete={this.handleClickDelete} />
  }
}

const {func, oneOfType, shape, string} = PropTypes

DeleteButton.propTypes = {
  onClickDelete: func.isRequired,
}

DeleteConfirmButtons.propTypes = {
  item: oneOfType([(string, shape())]),
  onDelete: func.isRequired,
}

export default OnClickOutside(DeleteConfirmButtons)
