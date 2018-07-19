import React, {Component} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import {ComponentColor, ComponentSize, ButtonShape} from 'src/reusable_ui/types'

interface Props {
  customClass?: string
  color?: ComponentColor
  size?: ComponentSize
  shape?: ButtonShape
  disabled?: boolean
  buttons: string[]
  activeButton: string
  onChange: (RadioButton) => void
}

class RadioButtons extends Component<Props> {
  public static defaultProps = {
    color: ComponentColor.Default,
    size: ComponentSize.Small,
    shape: ButtonShape.Default,
    disabled: false,
  }

  public render() {
    return <div className={this.containerClassName}>{this.buttons}</div>
  }

  private get buttons(): JSX.Element[] {
    const {buttons} = this.props

    return buttons.map(button => (
      <div
        key={button}
        className={this.buttonClassName(button)}
        onClick={this.handleButtonClick(button)}
      >
        {this.buttonText(button)}
      </div>
    ))
  }

  private buttonText = (button): JSX.Element => {
    if (_.startsWith(button, 'icon')) {
      return <span className={button} />
    }

    return <>{button}</>
  }

  private get containerClassName(): string {
    const {color, size, shape, customClass} = this.props

    return classnames(
      `radio-buttons radio-buttons-${color} radio-buttons-${size}`,
      {
        'radio-buttons-square': shape === ButtonShape.Square,
        'radio-buttons-stretch': shape === ButtonShape.StretchToFit,
        [customClass]: customClass,
      }
    )
  }

  private buttonClassName = (button: string): string => {
    const {activeButton} = this.props

    return classnames('radio-button', {
      active: _.isEqual(button, activeButton),
    })
  }

  private handleButtonClick = (button: string) => (): void => {
    const {onChange, disabled} = this.props
    if (disabled) {
      return
    }

    onChange(button)
  }
}

export default RadioButtons
