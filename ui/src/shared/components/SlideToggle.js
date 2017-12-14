import React, {Component, PropTypes} from 'react'

class SlideToggle extends Component {
  constructor(props) {
    super(props)

    this.state = {
      active: props.active,
    }
  }

  componentWillReceiveProps(nextProps) {
    this.setState({active: nextProps.active})
  }

  handleClick = () => {
    const {onToggle} = this.props

    this.setState({active: !this.state.active}, () => {
      onToggle(this.state.active)
    })
  }

  render() {
    const {size} = this.props
    const {active} = this.state

    const classNames = active
      ? `slide-toggle slide-toggle__${size} active`
      : `slide-toggle slide-toggle__${size}`

    return (
      <div className={classNames} onClick={this.handleClick}>
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
}

export default SlideToggle
