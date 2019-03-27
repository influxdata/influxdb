// Libraries
import React, {Component, ChangeEvent, KeyboardEvent} from 'react'

// Components
import {Input, Radio} from '@influxdata/clockface'

// Styles
import './AutoInput.scss'

// Types
import {ButtonShape} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

enum Mode {
  Auto = 'auto',
  Custom = 'custom',
}

interface Props {
  name: string
  inputPlaceholder: string
  onChange: (value: number) => void
  value?: number
  min?: number
  max?: number
}

interface State {
  inputMode: Mode
  inputValue: string
}

@ErrorHandling
export default class AutoInput extends Component<Props, State> {
  constructor(props) {
    super(props)

    const inputMode = props.value ? Mode.Custom : Mode.Auto

    this.state = {
      inputMode,
      inputValue: props.value || 0,
    }
  }

  public render() {
    const {inputMode} = this.state
    const {name} = this.props

    return (
      <div className="auto-input">
        <div className="auto-input--radio">
          <Radio shape={ButtonShape.StretchToFit}>
            <Radio.Button
              active={inputMode === Mode.Auto}
              id={`auto--${name}`}
              titleText="Decide for me"
              value={Mode.Auto}
              onClick={this.handleRadioClick}
            >
              Auto
            </Radio.Button>
            <Radio.Button
              active={inputMode === Mode.Custom}
              id={`custom--${name}`}
              titleText="I want to specify my own value"
              value={Mode.Custom}
              onClick={this.handleRadioClick}
            >
              Custom
            </Radio.Button>
          </Radio>
        </div>
        {this.input}
      </div>
    )
  }

  private get input(): JSX.Element {
    const {inputMode, inputValue} = this.state
    const {inputPlaceholder} = this.props

    if (inputMode === Mode.Custom) {
      return (
        <div className="auto-input--input">
          <Input
            placeholder={inputPlaceholder}
            value={`${inputValue}`}
            onChange={this.handleInputChange}
            onKeyPress={this.handleInputKeyPress}
            onBlur={this.emitValue}
            autoFocus={true}
          />
        </div>
      )
    }
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {max, min} = this.props

    let inputValue = e.target.value

    if (Number(inputValue) < min) {
      inputValue = String(min)
    } else if (Number(inputValue) > max) {
      inputValue = String(max)
    }

    this.setState({inputValue})
  }

  private handleRadioClick = (inputMode: Mode) => {
    if (inputMode === this.state.inputMode) {
      return
    }

    this.setState({inputMode, inputValue: ''}, this.emitValue)
  }

  private handleInputKeyPress = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      this.emitValue()
    }
  }

  private emitValue = () => {
    const {onChange} = this.props
    const {inputValue} = this.state

    if (inputValue === '' || isNaN(Number(inputValue))) {
      onChange(null)
    } else {
      onChange(Number(inputValue))
    }
  }
}
