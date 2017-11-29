import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'

import OnClickOutside from 'shared/components/OnClickOutside'
import ConfirmButtons from 'shared/components/ConfirmButtons'

const DeleteButton = ({onClickDelete, buttonSize, icon, square}) =>
  <button
    className={classnames('btn btn-danger table--show-on-row-hover', {
      [buttonSize]: buttonSize,
      'btn-square': square,
    })}
    onClick={onClickDelete}
  >
    {icon ? <span className={`icon ${icon}`} /> : 'Delete'}
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
    const {onDelete, item, buttonSize, icon, square} = this.props
    const {isConfirming} = this.state

    return isConfirming
      ? <ConfirmButtons
          onConfirm={onDelete}
          item={item}
          onCancel={this.handleCancel}
          buttonSize={buttonSize}
        />
      : <DeleteButton
          onClickDelete={this.handleClickDelete}
          buttonSize={buttonSize}
          icon={icon}
          square={square}
        />
  }
}

const {bool, func, oneOfType, shape, string} = PropTypes

DeleteButton.propTypes = {
  onClickDelete: func.isRequired,
  buttonSize: string,
  icon: string,
  square: bool,
}

DeleteConfirmButtons.propTypes = {
  item: oneOfType([(string, shape())]),
  onDelete: func.isRequired,
  buttonSize: string,
  square: bool,
  icon: string,
}

DeleteConfirmButtons.defaultProps = {
  buttonSize: 'btn-sm',
}

export default OnClickOutside(DeleteConfirmButtons)
