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
  }

  useFixedValue = () => {
    this.setState({useCustomValue: false, customValue: ''}, () =>
      this.setValue()
    )
  }

  useCustomValue = () => {
    this.setState({useCustomValue: true}, () => this.setValue())
  }

  handleClickToggle = () => {
    const useCustomValueNext = !this.state.useCustomValue
    if (useCustomValueNext) {
      this.useCustomValue()
      this.customValueInput.focus()
    } else {
      this.useFixedValue()
    }
  }

  handleFocusCustomValueInput = () => {
    this.isCustomValueInputFocused = true
    this.useCustomValue()
  }

  handleChangeCustomValue = e => {
    this.setCustomValue(e.target.value)
  }

  handleKeyDownCustomValueInput = e => {
    if (e.key === 'Enter' || e.key === 'Tab') {
      if (e.key === 'Enter') {
        this.customValueInput.blur()
      }
      this.considerResetCustomValue()
    }
  }

  handleClickOutsideInput = e => {
    if (
      e.target.id !== this.grooveKnob.id &&
      e.target.id !== this.grooveKnobContainer.id &&
      this.isCustomValueInputFocused
    ) {
      this.considerResetCustomValue()
    }
  }

  considerResetCustomValue = () => {
    const customValue = this.customValueInput.value.trim()

    this.setState({customValue})

    if (customValue === '') {
      this.useFixedValue()
    }

    this.isCustomValueInputFocused = false
  }

  setCustomValue = value => {
    this.setState({customValue: value}, this.setValue)
  }

  setValue = () => {
    const {onSetValue} = this.props
    const {useCustomValue, fixedValue, customValue} = this.state

    if (useCustomValue) {
      onSetValue(customValue)
    } else {
      onSetValue(fixedValue)
    }
  }

  handleInputRef = el => (this.customValueInput = el)

  render() {
    const {fixedPlaceholder, customPlaceholder, type, min} = this.props
    const {useCustomValue, customValue} = this.state

    return (
      <div
        className={classnames('opt-in', {
          'right-toggled': useCustomValue,
        })}
      >
        <ClickOutsideInput
          id={this.id}
          min={min}
          type={type}
          customValue={customValue}
          onGetRef={this.handleInputRef}
          customPlaceholder={customPlaceholder}
          onChange={this.handleChangeCustomValue}
          onFocus={this.handleFocusCustomValueInput}
          onKeyDown={this.handleKeyDownCustomValueInput}
          handleClickOutsideInput={this.handleClickOutsideInput}
        />
        <div
          className="opt-in--container"
          id={this.id}
          ref={el => (this.grooveKnobContainer = el)}
        >
          <div
            className="opt-in--groove-knob"
            id={this.id}
            ref={el => (this.grooveKnob = el)}
            onClick={this.handleClickToggle}
          />
          <div className="opt-in--label" onClick={this.useFixedValue}>
            {fixedPlaceholder}
          </div>
        </div>
      </div>
    )
  }
}

OptIn.defaultProps = {
  fixedValue: '',
  customPlaceholder: 'Custom Value',
  fixedPlaceholder: 'auto',
  customValue: '',
}

const {func, oneOf, string} = PropTypes

OptIn.propTypes = {
  min: string,
  fixedPlaceholder: string,
  fixedValue: string,
  customPlaceholder: string,
  customValue: string,
  onSetValue: func.isRequired,
  type: oneOf(['text', 'number']),
}

export default OptIn
