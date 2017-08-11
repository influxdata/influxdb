import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

// these help ensure that blur and toggle events don't both change toggle's side
const RESET_TIMEOUT = 300
const TOGGLE_CLICKED_TIMEOUT = 20
const BLUR_FOCUS_GAP_TIMEOUT = 10

class OptIn extends Component {
  constructor(props) {
    super(props)

    const {customValue, fixedValue} = props

    this.state = {
      useCustomValue: customValue !== '',
      fixedValue,
      customValue,
      wasToggleClicked: false,
      wasCustomValueInputBlurred: false,
      resetTimeoutID: null,
      toggleTimeoutID: null,
      blurTimeoutID: null,
    }

    this.useFixedValue = ::this.useFixedValue
    this.toggleValue = ::this.toggleValue
    this.useCustomValue = ::this.useCustomValue
    this.handleClickFixedValueField = ::this.handleClickFixedValueField
    this.handleClickToggle = ::this.handleClickToggle
    this.handleFocusCustomValueInput = ::this.handleFocusCustomValueInput
    this.handleBlurCustomValueInput = ::this.handleBlurCustomValueInput
    this.handleChangeCustomValue = ::this.handleChangeCustomValue
    this.handleKeyPressCustomValueInput = ::this.handleKeyPressCustomValueInput
    this.setCustomValue = ::this.setCustomValue
    this.setValue = ::this.setValue
  }

  componentWillUnmount() {
    clearTimeout(this.state.resetTimeoutID)
    clearTimeout(this.state.toggleTimeoutID)
    clearTimeout(this.state.blurTimeoutID)
  }

  useFixedValue() {
    this.setState({useCustomValue: false}, this.setValue)
  }

  toggleValue() {
    const useCustomValueNext = !this.state.useCustomValue
    if (useCustomValueNext && !this.state.wasCustomValueInputBlurred) {
      this.useCustomValue()
    } else {
      this.useFixedValue()
    }
  }

  useCustomValue() {
    this.setState({useCustomValue: true}, () => {
      if (
        this.state.wasToggleClicked &&
        !this.state.wasCustomValueInputBlurred
      ) {
        this.customValueInput.focus()
      }
      this.setValue()
    })
  }

  handleClickFixedValueField() {
    return () => this.useFixedValue()
  }

  handleClickToggle() {
    return () => {
      this.setState({wasToggleClicked: true}, () => {
        this.toggleValue()
      })

      const toggleTimeoutID = setTimeout(() => {
        this.setState({wasToggleClicked: false})
      }, TOGGLE_CLICKED_TIMEOUT)

      this.setState({toggleTimeoutID})
    }
  }

  handleFocusCustomValueInput() {
    return () => this.useCustomValue()
  }

  handleBlurCustomValueInput() {
    return e => {
      this.setState(
        {wasCustomValueInputBlurred: true, customValue: e.target.value.trim()},
        () => {
          if (this.state.customValue === '') {
            const blurTimeoutID = setTimeout(() => {
              if (!this.state.wasToggleClicked) {
                this.useFixedValue()
              }
            }, BLUR_FOCUS_GAP_TIMEOUT)

            this.setState({blurTimeoutID})
          }
        }
      )
    }
  }

  handleChangeCustomValue() {
    return e => {
      this.setCustomValue(e.target.value)
    }
  }

  handleKeyPressCustomValueInput() {
    return e => {
      if (e.key === 'Enter') {
        this.customValueInput.blur()
      }
    }
  }

  setCustomValue(value) {
    this.setState({customValue: value}, this.setValue)
  }

  setValue() {
    const {onSetValue} = this.props
    const {useCustomValue, fixedValue, customValue} = this.state
    if (useCustomValue) {
      onSetValue(customValue)
    } else {
      this.setState({customValue: ''})
      onSetValue(fixedValue)
    }

    // reset UI interaction state-tracking values & prevent blur + click
    const resetTimeoutID = setTimeout(() => {
      this.setState({
        wasToggleClicked: false,
        wasCustomValueInputBlurred: false,
      })
    }, RESET_TIMEOUT)

    this.setState({resetTimeoutID})
  }

  render() {
    const {fixedPlaceholder, customPlaceholder, type} = this.props
    const {useCustomValue, customValue} = this.state

    return (
      <div
        className={classnames('opt-in', {
          'right-toggled': useCustomValue,
        })}
      >
        <input
          className="form-control input-sm"
          type={type}
          name={customPlaceholder}
          ref={el => (this.customValueInput = el)}
          value={customValue}
          onFocus={this.handleFocusCustomValueInput()}
          onBlur={this.handleBlurCustomValueInput()}
          onChange={this.handleChangeCustomValue()}
          onKeyPress={this.handleKeyPressCustomValueInput()}
          placeholder={customPlaceholder}
        />
        <div
          className="opt-in--groove-knob-container"
          onClick={this.handleClickToggle()}
        >
          <div className="opt-in--groove-knob" />
        </div>
        <div
          className="opt-in--left-label"
          onClick={this.handleClickFixedValueField()}
        >
          {fixedPlaceholder}
        </div>
      </div>
    )
  }
}

OptIn.defaultProps = {
  fixedPlaceholder: 'auto',
  fixedValue: '',
  customPlaceholder: 'Custom Value',
  customValue: '',
}
const {func, oneOf, string} = PropTypes

OptIn.propTypes = {
  fixedPlaceholder: string,
  fixedValue: string,
  customPlaceholder: string,
  customValue: string,
  onSetValue: func.isRequired,
  type: oneOf(['text', 'number']),
}

export default OptIn
