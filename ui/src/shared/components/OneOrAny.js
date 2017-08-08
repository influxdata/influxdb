import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

// these help ensure that blur and toggle events don't both change toggle's side
const RESET_TIMEOUT = 300
const TOGGLE_CLICKED_TIMEOUT = 20
const BLUR_FOCUS_GAP_TIMEOUT = 10

class OneOrAny extends Component {
  constructor(props) {
    super(props)

    const {value, leftValue} = props
    const useRightValue = value !== ''

    this.state = {
      useRightValue,
      leftValue,
      rightValue: value || '',
      leftValueFieldClicked: false,
      toggleClicked: false,
      rightValueInputBlurred: false,
      // rightValueInputClicked: false, // TODO: implement right input clickability
    }

    this.useLeftValue = ::this.useLeftValue
    this.toggleValue = ::this.toggleValue
    this.useRightValue = ::this.useRightValue
    this.handleClickLeftValueField = ::this.handleClickLeftValueField
    this.handleClickToggle = ::this.handleClickToggle
    this.handleBlurRightValueInput = ::this.handleBlurRightValueInput
    this.handleChangeRightValue = ::this.handleChangeRightValue
    this.setRightValue = ::this.setRightValue
    this.setValue = ::this.setValue
  }

  useLeftValue() {
    this.setState({useRightValue: false}, this.setValue)
  }

  toggleValue() {
    const useRightValueNext = !this.state.useRightValue
    if (useRightValueNext && !this.state.rightValueInputBlurred) {
      this.useRightValue()
    } else {
      this.useLeftValue()
    }
  }

  useRightValue() {
    this.setState({useRightValue: true}, () => {
      if (this.state.toggleClicked && !this.state.rightValueInputBlurred) {
        // TODO: || if this.state.rightValueInputClicked
        this.rightValueInput.focus()
      }
      this.setValue()
    })
  }

  handleClickLeftValueField() {
    return () => {
      this.setState({leftValueFieldClicked: true}, this.useLeftValue)
    }
  }

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

  handleBlurRightValueInput() {
    return e => {
      this.setState(
        {rightValueInputBlurred: true, rightValue: e.target.value.trim()},
        () => {
          if (this.state.rightValue === '') {
            setTimeout(() => {
              if (!this.state.toggleClicked) {
                this.useLeftValue()
              }
            }, BLUR_FOCUS_GAP_TIMEOUT)
          }
        }
      )
    }
  }

  handleChangeRightValue() {
    return e => {
      this.setRightValue(e.target.value)
    }
  }

  setRightValue(value) {
    this.setState({rightValue: value}, this.setValue)
  }

  setValue() {
    const {onSetValue} = this.props
    const {useRightValue, leftValue, rightValue} = this.state

    if (useRightValue) {
      onSetValue({value: rightValue})
    } else {
      this.setState({rightValue: ''})
      onSetValue({value: leftValue})
    }

    // reset UI interaction state-tracking values & prevent blur + click
    setTimeout(() => {
      this.setState({toggleClicked: false, rightValueInputBlurred: false})
    }, RESET_TIMEOUT)
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
          onClick={this.handleClickLeftValueField()}
        >
          {leftLabel}
        </div>
        <div
          className="one-or-any--groove-knob-container"
          onClick={this.handleClickToggle()}
        >
          <div className="one-or-any--groove-knob" />
        </div>
        <input
          className="form-control input-sm"
          type="number"
          name="rightValueInput"
          id="rightValueInput"
          ref={el => (this.rightValueInput = el)}
          value={rightValue}
          onClick={() => {
            // TODO: you can't 'click' a disabled button -- find another solution
            // this.useRightValue()
          }}
          onBlur={this.handleBlurRightValueInput()}
          onChange={this.handleChangeRightValue()}
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
  value: '',
}
const {func, string} = PropTypes

OneOrAny.propTypes = {
  leftLabel: string,
  leftValue: string,
  rightLabel: string,
  value: string,
  onSetValue: func.isRequired,
}

export default OneOrAny
