import React, {PureComponent} from 'react'
import {Dropdown} from 'src/shared/components/Dropdown'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'
import {
  FORMAT_OPTIONS,
  TIME_FORMAT_CUSTOM,
  TIME_FORMAT_DEFAULT,
  TIME_FORMAT_TOOLTIP_LINK,
} from 'src/shared/constants/tableGraph'
import {ErrorHandling} from 'src/shared/decorators/errors'

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

@ErrorHandling
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

  public handleChangeFormat = e => {
    const format = e.target.value
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
    const tipContent = `For information on formatting, see <br/><a href="#">${TIME_FORMAT_TOOLTIP_LINK}</a>`

    const formatOption = FORMAT_OPTIONS.find(op => op.text === format)
    const showCustom = !formatOption || customFormat

    return (
      <div className="form-group col-xs-12">
        <label>
          Time Format
          {showCustom && (
            <a href={TIME_FORMAT_TOOLTIP_LINK} target="_blank">
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
          <input
            type="text"
            spellCheck={false}
            placeholder="Enter custom format..."
            value={format}
            data-test="custom-time-format"
            className="form-control input-sm custom-time-format"
            onChange={this.handleChangeFormat}
          />
        )}
      </div>
    )
  }
}

export default GraphOptionsTimeFormat
