import React, {PureComponent} from 'react'
import classnames from 'classnames'

import OnClickOutside from 'src/shared/components/OnClickOutside'
import ConfirmButton from 'src/shared/components/ConfirmButton'

type Item = Object | string

interface ConfirmButtonsProps {
  onConfirm: (item: Item) => void
  item: Item
  onCancel: (item: Item) => void
  buttonSize?: string
  isDisabled?: boolean
  onClickOutside?: (item: Item) => void
  confirmLeft?: boolean
  confirmHoverText?: string
}

class ConfirmButtons extends PureComponent<ConfirmButtonsProps, {}> {
  constructor(props) {
    super(props)
  }

  public static defaultProps: Partial<ConfirmButtonsProps> = {
    buttonSize: 'btn-sm',
    onClickOutside: () => {},
  }

  handleConfirm = item => () => {
    this.props.onConfirm(item)
  }

  handleCancel = item => () => {
    this.props.onCancel(item)
  }

  handleClickOutside = () => {
    this.props.onClickOutside(this.props.item)
  }

  render() {
    const {
      item,
      buttonSize,
      isDisabled,
      confirmLeft,
      confirmHoverText,
    } = this.props
    const hoverText = confirmHoverText || 'Save'

    const confirmClass = classnames('btn btn-success btn-square', {
      [buttonSize]: buttonSize,
    })

    const cancelClass = classnames('btn btn-info btn-square', {
      [buttonSize]: buttonSize,
    })
    return confirmLeft
      ? <div className="confirm-buttons">
          <ConfirmButton
            customClass={confirmClass}
            disabled={isDisabled}
            confirmAction={this.handleConfirm(item)}
            icon="icon checkmark"
            hoverText={isDisabled ? `Cannot ${hoverText}` : hoverText}
          />
          <ConfirmButton
            customClass={cancelClass}
            confirmAction={this.handleCancel(item)}
            icon="icon remove"
          />
        </div>
      : <div className="confirm-buttons">
          <ConfirmButton
            customClass={cancelClass}
            confirmAction={this.handleCancel(item)}
            icon="icon remove"
          />
          <ConfirmButton
            customClass={confirmClass}
            disabled={isDisabled}
            confirmAction={this.handleConfirm(item)}
            icon="icon checkmark"
            hoverText={isDisabled ? `Cannot ${hoverText}` : hoverText}
          />
        </div>
  }
}

export default OnClickOutside(ConfirmButtons)
