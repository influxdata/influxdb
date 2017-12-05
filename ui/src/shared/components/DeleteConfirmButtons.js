import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'

import OnClickOutside from 'shared/components/OnClickOutside'
import ConfirmButtons from 'shared/components/ConfirmButtons'

const DeleteButton = ({
  onClickDelete,
  buttonSize,
  icon,
  square,
  text,
  disabled,
}) =>
  <button
    className={classnames('btn btn-danger table--show-on-row-hover', {
      [buttonSize]: buttonSize,
      'btn-square': square,
      disabled,
    })}
    onClick={onClickDelete}
  >
    {icon ? <span className={`icon ${icon}`} /> : null}
    {square ? null : text}
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
    const {
      onDelete,
      item,
      buttonSize,
      icon,
      square,
      text,
      disabled,
    } = this.props
    const {isConfirming} = this.state

    if (square && !icon) {
      console.error(
        'DeleteButton component requires both icon if passing in square.'
      )
    }

    return isConfirming
      ? <ConfirmButtons
          onConfirm={onDelete}
          item={item}
          onCancel={this.handleCancel}
          buttonSize={buttonSize}
        />
      : <DeleteButton
          text={text}
          onClickDelete={disabled ? () => {} : this.handleClickDelete}
          buttonSize={buttonSize}
          icon={icon}
          square={square}
          disabled={disabled}
        />
  }
}

const {bool, func, oneOfType, shape, string} = PropTypes

DeleteButton.propTypes = {
  onClickDelete: func.isRequired,
  buttonSize: string,
  icon: string,
  square: bool,
  disabled: bool,
  text: string.isRequired,
}

DeleteButton.defaultProps = {
  text: 'Delete',
}

DeleteConfirmButtons.propTypes = {
  text: string,
  item: oneOfType([(string, shape())]),
  onDelete: func.isRequired,
  buttonSize: string,
  square: bool,
  icon: string,
  disabled: bool,
}

DeleteConfirmButtons.defaultProps = {
  buttonSize: 'btn-sm',
}

export default OnClickOutside(DeleteConfirmButtons)
