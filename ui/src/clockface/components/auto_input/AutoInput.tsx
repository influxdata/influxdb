// Libraries
import React, {Component, ChangeEvent} from 'react'

// Components
import {Input, InputType, Radio, ButtonShape} from 'src/clockface'

// Styles
import './AutoInput.scss'

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
  inputValue: number
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
              id={`auto-input--auto--${name}`}
              key={`auto-input--auto--${name}`}
              titleText="Decide for me"
              value={Mode.Auto}
              onClick={this.handleRadioClick}
            >
              Auto
            </Radio.Button>
            <Radio.Button
              active={inputMode === Mode.Custom}
              id={`auto-input--custom--${name}`}
              key={`auto-input--auto--${name}`}
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
    const {min, max, inputPlaceholder} = this.props

    if (inputMode === Mode.Custom) {
      return (
        <div className="auto-input--input">
          <Input
            type={InputType.Number}
            min={min}
            max={max}
            placeholder={inputPlaceholder}
            value={`${inputValue}`}
            onChange={this.handleInputChange}
            onBlur={this.handleInputBlur}
            autoFocus={true}
          />
        </div>
      )
    }
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const inputValue = Number(e.target.value)

    this.setState({inputValue})
  }

  private handleRadioClick = (inputMode: Mode) => {
    const {onChange} = this.props

    if (inputMode === Mode.Custom) {
      this.setState({inputMode, inputValue: 0})
      onChange(null)
    } else {
      this.setState({inputMode, inputValue: null})
    }
  }

  private handleInputBlur = (e: ChangeEvent<HTMLInputElement>) => {
    const {onChange} = this.props
    const inputValue = Number(e.target.value)

    onChange(inputValue)
  }
}
