// Libraries
import React, {Component, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {Button, IconFont, ButtonShape} from '@influxdata/clockface'
import {Input} from 'src/clockface'
import Swatch from 'src/clockface/components/color_picker/ColorPickerSwatch'

// Constants
import {colors} from 'src/clockface/constants/colors'

// Styles
import 'src/clockface/components/color_picker/ColorPicker.scss'

interface Props {
  selectedHex: string
  onSelect: (hex: string) => void
  maintainInputFocus?: boolean
}

interface State {
  inputValue: string
}

export default class ColorPicker extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      inputValue: this.props.selectedHex || '',
    }
  }

  componentDidUpdate(prevProps) {
    if (prevProps.selectedHex !== this.props.selectedHex) {
      this.setState({inputValue: this.props.selectedHex})
    }
  }

  render() {
    const {maintainInputFocus} = this.props
    const {inputValue} = this.state

    return (
      <div className="color-picker">
        <div className="color-picker--swatches">
          {colors.map(color => (
            <Swatch
              key={color.name}
              hex={color.hex}
              name={color.name}
              onClick={this.handleSwatchClick}
            />
          ))}
        </div>
        <div className="color-picker--form">
          {this.selectedColor}
          <Input
            customClass="color-picker--input"
            placeholder="#000000"
            value={inputValue}
            onChange={this.handleInputChange}
            maxLength={7}
            onBlur={this.handleInputBlur}
            autoFocus={maintainInputFocus}
          />
          <Button
            icon={IconFont.Refresh}
            shape={ButtonShape.Square}
            onClick={this.handleRandomizeColor}
          />
        </div>
      </div>
    )
  }

  private handleSwatchClick = (hex: string): void => {
    const {onSelect} = this.props

    this.setState({inputValue: hex})
    onSelect(hex)
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const acceptedChars = [
      '#',
      'a',
      'b',
      'c',
      'd',
      'e',
      'f',
      '0',
      '1',
      '2',
      '3',
      '4',
      '5',
      '6',
      '7',
      '8',
      '9',
    ]

    const trimmedValue = e.target.value.trim()
    const inputValue = trimmedValue
      .split('')
      .filter(char => acceptedChars.includes(char.toLowerCase()))
      .join('')

    this.setState({inputValue})
  }

  private handleInputBlur = (e: ChangeEvent<HTMLInputElement>) => {
    const {maintainInputFocus} = this.props

    if (maintainInputFocus) {
      e.target.focus()
    }
  }

  private handleRandomizeColor = (): void => {
    const {onSelect} = this.props
    const {hex} = _.sample(colors)

    this.setState({inputValue: hex})
    onSelect(hex)
  }

  private get selectedColor(): JSX.Element {
    const {inputValue} = this.state

    return (
      <div
        className="color-picker--selected"
        style={{backgroundColor: inputValue}}
      />
    )
  }
}
