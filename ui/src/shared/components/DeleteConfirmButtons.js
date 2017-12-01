import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'

import OnClickOutside from 'shared/components/OnClickOutside'
import ConfirmButtons from 'shared/components/ConfirmButtons'

const DeleteButton = ({onClickDelete, buttonSize, text, disabled}) =>
  <button
    className={classnames('btn btn-danger table--show-on-row-hover', {
      [buttonSize]: buttonSize,
      disabled,
    })}
    onClick={onClickDelete}
  >
    {text}
  </button>

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
    const {onDelete, item, buttonSize, text, disabled} = this.props
    const {isConfirming} = this.state

    return isConfirming
      ? <ConfirmButtons
          onConfirm={onDelete}
          item={item}
          onCancel={this.handleCancel}
          buttonSize={buttonSize}
        />
      : <DeleteButton
          text={text}
          onClickDelete={this.handleClickDelete}
          buttonSize={buttonSize}
          disabled={disabled}
        />
  }
}

const {bool, func, oneOfType, shape, string} = PropTypes

DeleteButton.propTypes = {
  text: string.isRequired,
  onClickDelete: func.isRequired,
  buttonSize: string,
  disabled: bool,
}

DeleteButton.defaultProps = {
  text: 'Delete',
}

DeleteConfirmButtons.propTypes = {
  text: string,
  item: oneOfType([(string, shape())]),
  onDelete: func.isRequired,
  buttonSize: string,
  disabled: bool,
}

DeleteConfirmButtons.defaultProps = {
  buttonSize: 'btn-sm',
}

export default OnClickOutside(DeleteConfirmButtons)
