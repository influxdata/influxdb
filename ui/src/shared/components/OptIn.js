import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

import uuid from 'node-uuid'

import ClickOutsideInput from 'shared/components/ClickOutsideInput'

class OptIn extends Component {
  constructor(props) {
    super(props)

    const {customValue, fixedValue} = props

    this.state = {
      useCustomValue: customValue !== '',
      fixedValue,
      customValue,
    }

    this.id = uuid.v4()
    this.isCustomValueInputFocused = false

    this.useFixedValue = ::this.useFixedValue
    this.useCustomValue = ::this.useCustomValue
    this.handleClickFixedValueField = ::this.handleClickFixedValueField
    this.handleClickToggle = ::this.handleClickToggle
    // this.handleFocusCustomValueInput = ::this.handleFocusCustomValueInput
    this.handleChangeCustomValue = ::this.handleChangeCustomValue
    this.handleKeyDownCustomValueInput = ::this.handleKeyDownCustomValueInput
    this.handleClickOutsideCustomValueInput = ::this
      .handleClickOutsideCustomValueInput
    this.considerResetCustomValue = ::this.considerResetCustomValue
    this.setCustomValue = ::this.setCustomValue
    this.setValue = ::this.setValue
  }

  useFixedValue() {
    this.setState({useCustomValue: false, customValue: ''}, () =>
      this.setValue()
    )
    // this.customValueInput.blur()
  }

  useCustomValue() {
    this.setState({useCustomValue: true}, () => this.setValue())
  }

  handleClickFixedValueField() {
    return () => this.useFixedValue()
  }

  handleClickToggle() {
    return () => {
      const useCustomValueNext = !this.state.useCustomValue
      if (useCustomValueNext) {
        this.useCustomValue()
        this.customValueInput.focus()
      } else {
        this.useFixedValue()
      }
    }
  }

  handleFocusCustomValueInput() {
    return () => {
      this.isCustomValueInputFocused = true
      this.useCustomValue()
    }
  }

  handleChangeCustomValue() {
    return e => {
      this.setCustomValue(e.target.value)
    }
  }

  handleKeyDownCustomValueInput() {
    return e => {
      if (e.key === 'Enter' || e.key === 'Tab') {
        if (e.key === 'Enter') {
          this.customValueInput.blur()
        }
        this.considerResetCustomValue()
      }
    }
  }

  handleClickOutsideCustomValueInput() {
    return e => {
      if (
        e.target.id !== this.grooveKnob.id &&
        e.target.id !== this.grooveKnobContainer.id &&
        this.isCustomValueInputFocused
      ) {
        this.considerResetCustomValue()
      }
    }
  }

  considerResetCustomValue() {
    const customValue = this.customValueInput.value.trim()

    this.setState({customValue})

    if (customValue === '') {
      this.useFixedValue()
    }

    this.isCustomValueInputFocused = false
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
      onSetValue(fixedValue)
    }
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
        <ClickOutsideInput
          id={this.id}
          type={type}
          customPlaceholder={customPlaceholder}
          customValue={customValue}
          onGetRef={el => (this.customValueInput = el)}
          onFocus={this.handleFocusCustomValueInput()}
          onChange={this.handleChangeCustomValue()}
          onKeyDown={this.handleKeyDownCustomValueInput()}
          handleClickOutsideCustomValueInput={this.handleClickOutsideCustomValueInput()}
        />

        <div
          className="opt-in--groove-knob-container"
          id={this.id}
          ref={el => (this.grooveKnobContainer = el)}
          onClick={this.handleClickToggle()}
        >
          <div
            className="opt-in--groove-knob"
            id={this.id}
            ref={el => (this.grooveKnob = el)}
          />
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
