import React, {PureComponent} from 'react'
import {Dropdown} from 'src/shared/components/Dropdown'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'
import {
  FORMAT_OPTIONS,
  TIME_FORMAT_CUSTOM,
  TIME_FORMAT_DEFAULT,
} from 'src/shared/constants/tableGraph'

interface TimeFormatOptions {
  text: string
}

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
      customFormat: false,
      format: this.props.timeFormat || TIME_FORMAT_DEFAULT,
    }
  }

  get onTimeFormatChange() {
    return this.props.onTimeFormatChange
  }

  public handleChangeFormat = format => {
    this.onTimeFormatChange(format)
    this.setState({format})
  }

  public handleChooseFormat = (formatOption: TimeFormatOptions) => {
    if (formatOption.text === TIME_FORMAT_CUSTOM) {
      this.setState({customFormat: true})
    } else {
      this.onTimeFormatChange(formatOption.text)
      this.setState({format: formatOption.text, customFormat: false})
    }
  }

  public render() {
    const {format, customFormat} = this.state
    const tipUrl = 'http://momentjs.com/docs/#/parsing/string-format/'
    const tipContent = `For information on formatting, see ${tipUrl}`

    const formatOption = FORMAT_OPTIONS.find(op => op.text === format)
    const showCustom = !formatOption || customFormat

    return (
      <div className="form-group col-xs-12">
        <label>
          Time Format
          {showCustom && (
            <a href={tipUrl} target="_blank">
              <QuestionMarkTooltip
                tipID="Time Axis Format"
                tipContent={tipContent}
              />
            </a>
          )}
        </label>
        <Dropdown
          items={FORMAT_OPTIONS}
          selected={showCustom ? TIME_FORMAT_CUSTOM : format}
          buttonColor="btn-default"
          buttonSize="btn-sm"
          className="dropdown-stretch"
          onChoose={this.handleChooseFormat}
        />
        {showCustom && (
          <div className="column-controls--section">
            <InputClickToEdit
              wrapperClass="field-controls-input "
              value={format}
              onBlur={this.handleChangeFormat}
              onChange={this.handleChangeFormat}
              placeholder="Enter custom format..."
              appearAsNormalInput={true}
            />
          </div>
        )}
      </div>
    )
  }
}

export default GraphOptionsTimeFormat
