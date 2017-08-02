import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

class GrooveKnob extends Component {
  constructor(props) {
    super(props)

    const {leftValue, rightValue} = props

    this.state = {
      leftValue,
      rightValue,
    }

    this.handleChangeRightValue = ::this.handleChangeRightValue
    this.handleToggleLeftValue = ::this.handleToggleLeftValue
    this.handleSetValues = ::this.handleSetValues
  }

  handleChangeRightValue(newValue) {
    this.setState({rightValue: newValue}, this.handleSetValues)
  }

  handleToggleLeftValue() {
    const {leftValue} = this.state

    this.setState({leftValue: !leftValue}, this.handleSetValues)
  }

  handleSetValues() {
    const {onSetValues} = this.props
    const {leftValue, rightValue} = this.state

    onSetValues({leftValue, rightValue})
  }

  render() {
    const {leftLabel, rightLabel} = this.props
    const {leftValue: useLeftValue, rightValue} = this.state

    return (
      <div
        className={classnames('one-or-any', {'use-right-value': !useLeftValue})}
      >
        <div className="one-or-any--auto">
          {leftLabel}
        </div>
        <div
          className="one-or-any--switch"
          onClick={this.handleToggleLeftValue}
        >
          <div className="one-or-any--groove-knob" />
        </div>
        <input
          className="form-control input-sm"
          type="number"
          name="rightValue"
          id="rightValue"
          value={rightValue === null ? '' : rightValue}
          onChange={e => this.handleChangeRightValue(e.target.value)}
          placeholder={rightLabel}
          disabled={useLeftValue}
        />
      </div>
    )
  }
}

GrooveKnob.defaultProps = {
  leftLabel: 'auto',
  leftValue: true,
  rightLabel: 'Custom Value',
  rightValue: null,
}
const {bool, func, number, string} = PropTypes

GrooveKnob.propTypes = {
  leftLabel: string,
  leftValue: bool,
  rightLabel: string,
  rightValue: number,
  onSetValues: func.isRequired,
}

export default GrooveKnob
