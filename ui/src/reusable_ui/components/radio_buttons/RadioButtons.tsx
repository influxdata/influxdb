import React, {Component} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import {ComponentColor, ComponentSize, ButtonShape} from 'src/reusable_ui/types'
import {
  COMPONENT_COLORS,
  COMPONENT_SIZES,
  BUTTON_SHAPES,
} from 'src/reusable_ui/constants'

interface Props {
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
    color: COMPONENT_COLORS.default,
    size: COMPONENT_SIZES.small,
    shape: BUTTON_SHAPES.none,
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
        {button}
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
