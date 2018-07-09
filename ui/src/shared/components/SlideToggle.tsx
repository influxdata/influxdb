import React, {Component} from 'react'
import classnames from 'classnames'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  active: boolean
  size?: string
  onToggle: (newState: boolean) => void
  disabled?: boolean
}

interface State {
  active: boolean
}

@ErrorHandling
class SlideToggle extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    size: 'sm',
  }

  constructor(props) {
    super(props)

    this.state = {
      active: this.props.active,
    }
  }

  public componentWillReceiveProps(nextProps) {
    if (nextProps.active !== this.props.active) {
      this.setState({active: nextProps.active})
    }
  }

  public handleClick = () => {
    const {onToggle, disabled} = this.props

    if (disabled) {
      return
    }

    this.setState({active: !this.state.active}, () => {
      onToggle(this.state.active)
    })
  }

  public render() {
    return (
      <div className={this.className} onClick={this.handleClick}>
        <div className="slide-toggle--knob" />
      </div>
    )
  }

  private get className(): string {
    const {size, disabled} = this.props
    const {active} = this.state

    return classnames(`slide-toggle slide-toggle__${size}`, {active, disabled})
  }
}

export default SlideToggle
