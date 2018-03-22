import React, {PureComponent, SFC} from 'react'

import classnames from 'classnames'

import OnClickOutside from 'src/shared/components/OnClickOutside'

type Item = object | string

interface ConfirmProps {
  buttonSize: string
  isDisabled: boolean
  onConfirm: () => void
  icon: string
  title: string
}

interface CancelProps {
  buttonSize: string
  onCancel: () => void
  icon: string
}
interface ConfirmButtonsProps {
  onConfirm: (item: Item) => void
  item: Item
  onCancel: (item: Item) => void
  buttonSize?: string
  isDisabled?: boolean
  onClickOutside?: (item: Item) => void
  confirmLeft?: boolean
  confirmTitle?: string
}

export const Confirm: SFC<ConfirmProps> = ({
  buttonSize,
  isDisabled,
  onConfirm,
  icon,
  title,
}) => (
  <button
    data-test="confirm"
    className={classnames('btn btn-success btn-square', {
      [buttonSize]: buttonSize,
    })}
    disabled={isDisabled}
    title={isDisabled ? `Cannot ${title}` : title}
    onClick={onConfirm}
  >
    <span className={icon} />
  </button>
)

export const Cancel: SFC<CancelProps> = ({buttonSize, onCancel, icon}) => (
  <button
    data-test="cancel"
    className={classnames('btn btn-info btn-square', {
      [buttonSize]: buttonSize,
    })}
    onClick={onCancel}
  >
    <span className={icon} />
  </button>
)

class ConfirmButtons extends PureComponent<ConfirmButtonsProps, {}> {
  public static defaultProps: Partial<ConfirmButtonsProps> = {
    buttonSize: 'btn-sm',
    confirmTitle: 'Save',
    onClickOutside: () => {},
  }

  constructor(props) {
    super(props)
  }

  public handleConfirm = item => () => {
    this.props.onConfirm(item)
  }

  public handleCancel = item => () => {
    this.props.onCancel(item)
  }

  public handleClickOutside = () => {
    this.props.onClickOutside(this.props.item)
  }

  public render() {
    const {item, buttonSize, isDisabled, confirmLeft, confirmTitle} = this.props

    return confirmLeft ? (
      <div className="confirm-buttons">
        <Confirm
          buttonSize={buttonSize}
          isDisabled={isDisabled}
          onConfirm={this.handleConfirm(item)}
          icon="icon checkmark"
          title={confirmTitle}
        />
        <Cancel
          buttonSize={buttonSize}
          onCancel={this.handleCancel(item)}
          icon="icon remove"
        />
      </div>
    ) : (
      <div className="confirm-buttons">
        <Cancel
          buttonSize={buttonSize}
          onCancel={this.handleCancel(item)}
          icon="icon remove"
        />
        <Confirm
          buttonSize={buttonSize}
          isDisabled={isDisabled}
          onConfirm={this.handleConfirm(item)}
          icon="icon checkmark"
          title={confirmTitle}
        />
      </div>
    )
  }
}

export default OnClickOutside(ConfirmButtons)
