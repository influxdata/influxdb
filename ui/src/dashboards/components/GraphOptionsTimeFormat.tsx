import React, {PureComponent} from 'react'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'
import {Dropdown} from 'src/shared/components/Dropdown'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'

interface TimeFormatOptions {
  text: string
}

const formatOptions: TimeFormatOptions[] = [
  {text: 'MM/DD/YYYY HH:mm:ss.ss'},
  {text: 'MM/DD/YYYY HH:mm'},
  {text: 'MM/DD/YYYY'},
  {text: 'h:mm:ss A'},
  {text: 'h:mm A'},
  {text: 'MMMM D, YYYY'},
  {text: 'MMMM D, YYYY h:mm A'},
  {text: 'dddd, MMMM D, YYYY h:mm A'},
  {text: 'Custom'},
]

interface Props {
  timeFormat: string
  onTimeFormatChange: (format: string) => void
}

interface State {
  customFormat: boolean
  format: string
}

class GraphOptionsTimeFormat extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = {
      format: this.props.timeFormat || formatOptions[0].text,
      customFormat: false,
    }
  }

  get onTimeFormatChange() {
    return this.props.onTimeFormatChange
  }

  handleChooseFormat = (formatOption: TimeFormatOptions) => {
    if (formatOption.text === 'Custom') {
      this.setState({customFormat: true})
    } else {
      this.setState({format: formatOption.text, customFormat: false})
      this.onTimeFormatChange(formatOption.text)
    }
  }

  render() {
    const {format, customFormat} = this.state
    const {onTimeFormatChange} = this.props
    const tipContent =
      'For information on formatting, see http://momentjs.com/docs/#/parsing/string-format/'

    const formatOption = formatOptions.find(op => op.text === format)
    const showCustom = !formatOption || customFormat

    return (
      <div className="form-group col-xs-12">
        <label>
          Time Format
          {customFormat &&
            <QuestionMarkTooltip
              tipID="Time Axis Format"
              tipContent={tipContent}
            />}
        </label>
        <Dropdown
          items={formatOptions}
          selected={showCustom ? 'Custom' : format}
          buttonColor="btn-default"
          buttonSize="btn-xs"
          className="dropdown-stretch"
          onChoose={this.handleChooseFormat}
        />
        {showCustom &&
          <div className="column-controls--section">
            <InputClickToEdit
              wrapperClass="column-controls-input "
              value={format}
              onUpdate={onTimeFormatChange}
              placeholder="Enter custom format..."
              appearAsNormalInput={true}
            />
          </div>}
      </div>
    )
  }
}

export default GraphOptionsTimeFormat
