import React, {PureComponent, SFC} from 'react'

import classnames from 'classnames'

import OnClickOutside from 'src/shared/components/OnClickOutside'
import {ErrorHandling} from 'src/shared/decorators/errors'

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
  title: string
}

interface ConfirmOrCancelProps {
  onConfirm: (item: Item) => void
  item: Item
  onCancel: (item: Item) => void
  buttonSize?: string
  isDisabled?: boolean
  onClickOutside?: (item: Item) => void
  reversed?: boolean
  confirmTitle?: string
  cancelTitle?: string
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
    className={classnames(
      'confirm-or-cancel--confirm btn btn-success btn-square',
      {
        [buttonSize]: buttonSize,
      }
    )}
    disabled={isDisabled}
    title={isDisabled ? `Cannot ${title}` : title}
    onClick={onConfirm}
  >
    <span className={icon} />
  </button>
)

export const Cancel: SFC<CancelProps> = ({
  buttonSize,
  onCancel,
  icon,
  title,
}) => (
  <button
    data-test="cancel"
    className={classnames('confirm-or-cancel--cancel btn btn-info btn-square', {
      [buttonSize]: buttonSize,
    })}
    onClick={onCancel}
    title={title}
  >
    <span className={icon} />
  </button>
)

@ErrorHandling
class ConfirmOrCancel extends PureComponent<ConfirmOrCancelProps, {}> {
  public static defaultProps: Partial<ConfirmOrCancelProps> = {
    buttonSize: 'btn-sm',
    confirmTitle: 'Save',
    cancelTitle: 'Cancel',
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
    const {
      item,
      buttonSize,
      isDisabled,
      reversed,
      confirmTitle,
      cancelTitle,
    } = this.props

    const className = `confirm-or-cancel${reversed ? ' reversed' : ''}`
    return (
      <div className={className}>
        <Cancel
          buttonSize={buttonSize}
          onCancel={this.handleCancel(item)}
          icon="icon remove"
          title={cancelTitle}
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

export default OnClickOutside(ConfirmOrCancel)
