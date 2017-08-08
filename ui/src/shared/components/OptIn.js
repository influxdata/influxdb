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
      // fixedValueFieldClicked: false,
      toggleClicked: false,
      customValueInputBlurred: false,
      // customValueInputClicked: false, // TODO: implement custom input clickability
    }

    this.useFixedValue = ::this.useFixedValue
    this.toggleValue = ::this.toggleValue
    this.useCustomValue = ::this.useCustomValue
    // this.handleClickFixedValueField = ::this.handleClickFixedValueField
    this.handleClickToggle = ::this.handleClickToggle
    this.handleBlurCustomValueInput = ::this.handleBlurCustomValueInput
    this.handleChangeCustomValue = ::this.handleChangeCustomValue
    this.handleKeyPressCustomValueInput = ::this.handleKeyPressCustomValueInput
    this.setCustomValue = ::this.setCustomValue
    this.setValue = ::this.setValue
  }

  useFixedValue() {
    this.setState({useCustomValue: false}, this.setValue)
  }

  toggleValue() {
    const useCustomValueNext = !this.state.useCustomValue
    if (useCustomValueNext && !this.state.customValueInputBlurred) {
      this.useCustomValue()
    } else {
      this.useFixedValue()
    }
  }

  useCustomValue() {
    this.setState({useCustomValue: true}, () => {
      if (this.state.toggleClicked && !this.state.customValueInputBlurred) {
        // TODO: || if this.state.customValueInputClicked
        this.customValueInput.focus()
      }
      this.setValue()
    })
  }

  // handleClickFixedValueField() {
  //   return () => {
  //     this.setState({fixedValueFieldClicked: true}, this.useFixedValue)
  //   }
  // }

  handleClickToggle() {
    return () => {
      this.setState({toggleClicked: true}, () => {
        this.toggleValue()
      })

      setTimeout(() => {
        this.setState({toggleClicked: false})
      }, TOGGLE_CLICKED_TIMEOUT)
    }
  }

  handleBlurCustomValueInput() {
    return e => {
      this.setState(
        {customValueInputBlurred: true, customValue: e.target.value.trim()},
        () => {
          if (this.state.customValue === '') {
            setTimeout(() => {
              if (!this.state.toggleClicked) {
                this.useFixedValue()
              }
            }, BLUR_FOCUS_GAP_TIMEOUT)
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
    setTimeout(() => {
      this.setState({toggleClicked: false, customValueInputBlurred: false})
    }, RESET_TIMEOUT)
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
          id={customPlaceholder}
          ref={el => (this.customValueInput = el)}
          value={customValue}
          onClick={() => {
            // TODO: you can't 'click' a disabled button -- find another solution
            // this.useCustomValue()
          }}
          onBlur={this.handleBlurCustomValueInput()}
          onChange={this.handleChangeCustomValue()}
          onKeyPress={this.handleKeyPressCustomValueInput()}
          placeholder={customPlaceholder}
          // disabled={!useCustomValue}
        />
        <div
          className="opt-in--groove-knob-container"
          onClick={this.handleClickToggle()}
        >
          <div className="opt-in--groove-knob" />
        </div>
        <div
          className="opt-in--left-label"
          onClick={() => {
            // this.handleClickFixedValueField() // TODO: re-enable once clickability of custom value input is enabled
          }}
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
