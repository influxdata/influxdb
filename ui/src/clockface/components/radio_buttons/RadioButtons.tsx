// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import RadioButton from 'src/clockface/components/radio_buttons/RadioButton'

// Types
import {ComponentColor, ComponentSize, ButtonShape} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Styles
import './RadioButtons.scss'

interface Props {
  children: JSX.Element[]
  customClass?: string
  color: ComponentColor
  size: ComponentSize
  shape: ButtonShape
}

@ErrorHandling
class Radio extends Component<Props> {
  public static defaultProps = {
    color: ComponentColor.Primary,
    size: ComponentSize.Small,
    shape: ButtonShape.Default,
  }

  public static Button = RadioButton

  public render() {
    const {children} = this.props

    this.validateChildCount()

    return <div className={this.containerClassName}>{children}</div>
  }

  private get containerClassName(): string {
    const {color, size, shape, customClass} = this.props

    return classnames('radio-buttons', {
      [`radio-buttons--${color}`]: color,
      [`radio-buttons--${size}`]: size,
      'radio-buttons--square': shape === ButtonShape.Square,
      'radio-buttons--stretch': shape === ButtonShape.StretchToFit,
      [customClass]: customClass,
    })
  }

  private validateChildCount = (): void => {
    const {children} = this.props
    const MINIMUM_CHILD_COUNT = 2

    if (React.Children.count(children) < MINIMUM_CHILD_COUNT) {
      throw new Error(
        '<Radio> requires at least 2 child elements. We recommend using <Radio.Button />'
      )
    }
  }
}

export default Radio
