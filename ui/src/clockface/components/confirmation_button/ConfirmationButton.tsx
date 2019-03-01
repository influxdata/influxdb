// Libraries
import React, {Component, RefObject} from 'react'
import classnames from 'classnames'

// Components
import {Button} from '@influxdata/clockface'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Types
import {
  ComponentColor,
  ComponentStatus,
  ComponentSize,
  ButtonShape,
  IconFont,
  ButtonType,
} from '@influxdata/clockface'

// Styles
import 'src/clockface/components/confirmation_button/ConfirmationButton.scss'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  confirmText: string
  onConfirm: (returnValue?: any) => void
  returnValue?: any
  text?: string
  size?: ComponentSize
  shape?: ButtonShape
  icon?: IconFont
  status?: ComponentStatus
  titleText?: string
  tabIndex?: number
  className?: string
  testID?: string
}

interface State {
  isTooltipVisible: boolean
}

@ErrorHandling
class ConfirmationButton extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    size: ComponentSize.Small,
    shape: ButtonShape.Default,
    status: ComponentStatus.Default,
    testID: 'confirmation-button',
  }

  public ref: RefObject<HTMLButtonElement> = React.createRef()

  constructor(props: Props) {
    super(props)

    this.state = {
      isTooltipVisible: false,
    }
  }

  public render() {
    const {
      text,
      titleText,
      tabIndex,
      size,
      shape,
      status,
      confirmText,
      icon,
      testID,
    } = this.props
    const {isTooltipVisible} = this.state

    return (
      <ClickOutside onClickOutside={this.handleHideTooltip}>
        <div className={this.className}>
          <Button
            text={text}
            titleText={titleText}
            tabIndex={tabIndex}
            color={ComponentColor.Danger}
            shape={shape}
            size={size}
            status={status}
            active={isTooltipVisible}
            onClick={this.handleButtonClick}
            icon={icon}
            type={ButtonType.Button}
            testID="delete-button"
          />
          <div className={this.tooltipClassName}>
            <div
              data-testid={testID}
              className="confirmation-button--tooltip-body"
              onClick={this.handleTooltipClick}
            >
              {confirmText}
            </div>
          </div>
        </div>
      </ClickOutside>
    )
  }

  private handleButtonClick = (): void => {
    this.setState({isTooltipVisible: !this.state.isTooltipVisible})
  }

  private handleHideTooltip = (): void => {
    this.setState({isTooltipVisible: false})
  }

  private handleTooltipClick = (): void => {
    const {returnValue, onConfirm} = this.props

    onConfirm(returnValue)
    this.handleHideTooltip()
  }

  private get className(): string {
    const {shape, className} = this.props

    return classnames('confirmation-button', {
      'confirmation-button--stretch': shape === ButtonShape.StretchToFit,
      [className]: className,
    })
  }

  private get tooltipClassName(): string {
    const {isTooltipVisible} = this.state

    return classnames('confirmation-button--tooltip', {
      visible: isTooltipVisible,
    })
  }
}

export default ConfirmationButton
