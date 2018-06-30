import React, {Component} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import {ComponentColor, ComponentSize, ButtonShape} from 'src/reusable_ui/types'
import {
  COMPONENT_COLORS,
  COMPONENT_SIZES,
  BUTTON_SHAPES,
} from 'src/reusable_ui/constants'

export interface RadioButton {
  text: string
  disabled?: boolean
}

interface Props {
  color?: ComponentColor
  size?: ComponentSize
  shape?: ButtonShape
  buttons: RadioButton[]
  activeButton: RadioButton
  onChange: (RadioButton) => void
}

class RadioButtons extends Component<Props> {
  public static defaultProps = {
    color: COMPONENT_COLORS.default,
    size: COMPONENT_SIZES.small,
    shape: BUTTON_SHAPES.none,
  }

  public render() {
    return <div className={this.containerClassName}>{this.buttons}</div>
  }

  private get buttons(): JSX.Element[] {
    const {buttons} = this.props

    return buttons.map(button => (
      <div
        key={button.text}
        className={this.buttonClassName(button)}
        onClick={this.handleButtonClick(button)}
      >
        {button.text}
      </div>
    ))
  }

  private get containerClassName(): string {
    const {color, size, shape} = this.props

    return classnames(
      `radio-buttons radio-buttons-${color} radio-buttons-${size}`,
      {
        'radio-buttons-square': shape === BUTTON_SHAPES.square,
        'radio-buttons-stretch': shape === BUTTON_SHAPES.stretchToFit,
      }
    )
  }

  private buttonClassName = (button: RadioButton): string => {
    const {activeButton} = this.props

    return classnames('radio-button', {
      disabled: button.disabled,
      active: _.isEqual(button, activeButton),
    })
  }

  private handleButtonClick = (button: RadioButton) => () => {
    const {onChange} = this.props
    if (button.disabled) {
      return
    }

    onChange(button)
  }
}

export default RadioButtons
