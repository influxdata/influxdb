import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class SlideToggle extends Component {
  constructor(props) {
    super(props)

    this.state = {
      active: props.active,
    }
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.active !== this.props.active) {
      this.setState({active: nextProps.active})
    }
  }

  handleClick = () => {
    const {onToggle, disabled} = this.props

    if (disabled) {
      return
    }

    this.setState({active: !this.state.active}, () => {
      onToggle(this.state.active)
    })
  }

  render() {
    const {size, disabled} = this.props
    const {active} = this.state

    const className = `slide-toggle slide-toggle__${size} ${
      active ? 'active' : ''
    } ${disabled ? 'disabled' : ''}`

    return (
      <div className={className} onClick={this.handleClick}>
        <div className="slide-toggle--knob" />
      </div>
    )
  }
}

const {bool, func, string} = PropTypes

SlideToggle.defaultProps = {
  size: 'sm',
}
SlideToggle.propTypes = {
  active: bool,
  size: string,
  onToggle: func.isRequired,
  disabled: bool,
}

export default SlideToggle
