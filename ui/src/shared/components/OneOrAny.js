import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

// these help ensure that blur and toggle events don't both change knob's side
const TOGGLE_CLICKED_TIMEOUT = 20
const BLUR_FOCUS_GAP_TIMEOUT = 10

// TODO: separate useInput from useValue in order to perform focus() correctly
class OneOrAny extends Component {
  constructor(props) {
    super(props)

    const {useRightValue, leftValue, rightValue} = props

    this.state = {
      useRightValue,
      leftValue,
      rightValue,
      toggleWasClicked: false,
    }

    this.handleToggleLeftValue = ::this.handleToggleLeftValue
    this.handleToggleValue = ::this.handleToggleValue
    this.handleToggleRightValue = ::this.handleToggleRightValue
    this.handleBlurRight = ::this.handleBlurRight
    this.handleSetRightValue = ::this.handleSetRightValue
    this.handleSetValue = ::this.handleSetValue
  }

  handleToggleLeftValue() {
    return () => {
      this.setState({useRightValue: false}, this.handleSetValue)
    }
  }

  handleToggleValue() {
    return () => {
      const {useRightValue} = this.state
      this.setState(
        {useRightValue: !useRightValue, toggleWasClicked: true},
        this.handleSetValue
      )
      // this helps ensure that when the toggle is clicked, if the rightValue
      // input's blur event also fires, that only the expected behavior happens
      setTimeout(() => {
        this.setState({toggleWasClicked: false})
      }, TOGGLE_CLICKED_TIMEOUT)
    }
  }

  handleToggleRightValue() {
    return () => {
      this.setState({useRightValue: true}, this.handleSetValue)
    }
  }

  handleBlurRight() {
    return e => {
      const rightValue = e.target.value.trim()
      this.setState({rightValue}, () => {
        if (rightValue === '') {
          // this helps ensure that when the toggle is clicked, if the rightValue
          // input's blur event also fires, that only the expected behavior happens
          setTimeout(() => {
            if (!this.state.toggleWasClicked) {
              this.handleToggleLeftValue()()
            }
          }, BLUR_FOCUS_GAP_TIMEOUT)
        }
      })
    }
  }

  handleSetRightValue() {
    return e => {
      this.setState({rightValue: e.target.value}, this.handleSetValue)
    }
  }

  handleSetValue() {
    const {onSetValue} = this.props
    const {useRightValue, leftValue, rightValue} = this.state

    if (!useRightValue) {
      this.setState({rightValue: ''})
    }

    onSetValue({value: useRightValue ? rightValue : leftValue})
  }

  render() {
    const {leftLabel, rightLabel} = this.props
    const {useRightValue, rightValue} = this.state

    return (
      <div
        className={classnames('one-or-any', {'right-toggled': useRightValue})}
      >
        <div
          className="one-or-any--left-label"
          onClick={this.handleToggleLeftValue()}
        >
          {leftLabel}
        </div>
        <div
          className="one-or-any--groove-knob-container"
          onClick={this.handleToggleValue()}
        >
          <div className="one-or-any--groove-knob" />
        </div>
        <input
          className="form-control input-sm"
          type="number"
          name="rightValue"
          id="rightValue"
          value={rightValue}
          onClick={() => {
            // TODO: you can't 'click' a disabled button -- find another solution
            // this.handleToggleRightValue()
          }}
          onBlur={this.handleBlurRight()}
          onChange={this.handleSetRightValue()}
          placeholder={rightLabel}
          disabled={!useRightValue}
        />
      </div>
    )
  }
}

OneOrAny.defaultProps = {
  leftLabel: 'auto',
  leftValue: '',
  rightLabel: 'Custom Value',
  rightValue: '',
  useRightValue: false,
}
const {bool, func, string} = PropTypes

OneOrAny.propTypes = {
  useRightValue: bool,
  leftLabel: string,
  leftValue: string,
  rightLabel: string,
  rightValue: string,
  onSetValue: func.isRequired,
}

export default OneOrAny
