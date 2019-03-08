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

interface PassedProps {
  selectedHex: string
  onSelect: (hex: string, status?: ComponentStatus) => void
}

interface DefaultProps {
  maintainInputFocus?: boolean
  testID?: string
}

type Props = PassedProps & DefaultProps

interface State {
  inputValue: string
  status: string
}

export default class ColorPicker extends Component<Props, State> {
  public static defaultProps: DefaultProps = {
    maintainInputFocus: false,
    testID: 'color-picker',
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      inputValue: this.props.selectedHex || '',
      status: null,
    }
  }

  componentDidUpdate(prevProps) {
    if (prevProps.selectedHex !== this.props.selectedHex) {
      this.setState({inputValue: this.props.selectedHex})
    }
  }

  render() {
    const {maintainInputFocus, testID} = this.props
    const {inputValue} = this.state

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
            value={inputValue}
            onChange={this.handleInputChange}
            maxLength={7}
            onBlur={this.handleInputBlur}
            autoFocus={maintainInputFocus}
            status={this.inputStatus}
            testID={`${testID}--input`}
          />
          {this.selectedColor}
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

  private handleSwatchClick = (hex: string): void => {
    const {onSelect} = this.props

    this.setState({inputValue: hex})
    onSelect(hex, ComponentStatus.Valid)
  }

  private get inputStatus(): ComponentStatus {
    return this.state.status ? ComponentStatus.Error : ComponentStatus.Valid
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {onSelect} = this.props
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

    const status = validateHexCode(inputValue)
    const validity =
      status === null ? ComponentStatus.Valid : ComponentStatus.Error

    onSelect(inputValue, validity)
    this.setState({inputValue, status})
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
    onSelect(hex, ComponentStatus.Valid)
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

  private get errorMessage(): JSX.Element {
    const {testID} = this.props
    const {status} = this.state

    if (status) {
      return (
        <div className="color-picker--error" data-testid={`${testID}--error`}>
          <Error message={status} />
        </div>
      )
    }
  }
}
