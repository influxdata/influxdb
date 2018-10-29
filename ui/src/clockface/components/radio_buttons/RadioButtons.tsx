// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import RadioButton from 'src/clockface/components/radio_buttons/RadioButton'

// Types
import {ComponentColor, ComponentSize, ButtonShape} from 'src/clockface/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[]
  customClass?: string
  color?: ComponentColor
  size?: ComponentSize
  shape?: ButtonShape
}

@ErrorHandling
class Radio extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    color: ComponentColor.Default,
    size: ComponentSize.Small,
    shape: ButtonShape.Default,
  }

  public static Button = RadioButton

  public render() {
    const {children} = this.props

    this.validateChildCount()

    return (
      <div className={this.containerClassName}>
        {React.Children.map(children, (child: JSX.Element) => {
          if (this.childTypeIsValid(child)) {
            return <RadioButton {...child.props} key={child.props.id} />
          } else {
            throw new Error(
              '<Radio> expected children of type <Radio.Button />'
            )
          }
        })}
      </div>
    )
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

  private childTypeIsValid = (child: JSX.Element): boolean =>
    child.type === RadioButton
}

export default Radio
