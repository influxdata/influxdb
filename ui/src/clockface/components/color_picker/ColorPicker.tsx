// Libraries
import React, {Component, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {
  Button,
  IconFont,
  ButtonShape,
  ComponentStatus,
} from '@influxdata/clockface'
import {Input} from 'src/clockface'
import Swatch from 'src/clockface/components/color_picker/ColorPickerSwatch'
import Error from 'src/clockface/components/form_layout/FormElementError'

// Constants
import {colors} from 'src/clockface/constants/colors'

// Utils
import {validateHexCode} from 'src/configuration/utils/labels'

// Styles
import 'src/clockface/components/color_picker/ColorPicker.scss'

interface Props {
  color: string
  onChange: (color: string, status?: ComponentStatus) => void
  testID: string
  maintainInputFocus: boolean
}

interface State {
  errorMessage: string
}

export default class ColorPicker extends Component<Props, State> {
  public static defaultProps = {
    maintainInputFocus: false,
    testID: 'color-picker',
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      errorMessage: null,
    }
  }

  render() {
    const {maintainInputFocus, testID, color} = this.props

    return (
      <div className="color-picker" data-testid={testID}>
        <div className="color-picker--swatches">
          {colors.map(color => (
            <Swatch
              key={color.name}
              hex={color.hex}
              name={color.name}
              onClick={this.handleSwatchClick}
              testID={testID}
            />
          ))}
        </div>
        <div className="color-picker--form">
          <Input
            customClass="color-picker--input"
            placeholder="#000000"
            value={color}
            onChange={this.handleInputChange}
            maxLength={7}
            onBlur={this.handleInputBlur}
            autoFocus={maintainInputFocus}
            status={this.inputStatus}
            testID={`${testID}--input`}
          />
          {this.colorPreview}
          <Button
            icon={IconFont.Refresh}
            shape={ButtonShape.Square}
            onClick={this.handleRandomizeColor}
            titleText="I'm feeling lucky"
            testID={`${testID}--randomize`}
          />
        </div>
        {this.errorMessage}
      </div>
    )
  }

  private get inputStatus(): ComponentStatus {
    const {errorMessage} = this.state

    return errorMessage ? ComponentStatus.Error : ComponentStatus.Valid
  }

  private handleSwatchClick = (hex: string): void => {
    const {onChange} = this.props

    this.setState({errorMessage: null})
    onChange(hex, ComponentStatus.Valid)
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {onChange} = this.props
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
    const cleanedValue = trimmedValue
      .split('')
      .filter(char => acceptedChars.includes(char.toLowerCase()))
      .join('')

    const errorMessage = validateHexCode(cleanedValue)
    const status = errorMessage ? ComponentStatus.Error : ComponentStatus.Valid

    this.setState({errorMessage})
    onChange(cleanedValue, status)
  }

  private handleInputBlur = (e: ChangeEvent<HTMLInputElement>) => {
    const {maintainInputFocus} = this.props

    if (maintainInputFocus) {
      e.target.focus()
    }
  }

  private handleRandomizeColor = (): void => {
    const {onChange} = this.props
    const {hex} = _.sample(colors)

    this.setState({errorMessage: null})
    onChange(hex, ComponentStatus.Valid)
  }

  private get errorMessage(): JSX.Element {
    const {testID} = this.props
    const {errorMessage} = this.state

    if (errorMessage) {
      return (
        <div className="color-picker--error" data-testid={`${testID}--error`}>
          <Error message={errorMessage} />
        </div>
      )
    }
  }

  private get colorPreview(): JSX.Element {
    const {color} = this.props

    return (
      <div
        className="color-picker--selected"
        style={{backgroundColor: color}}
      />
    )
  }
}
