import React, {PureComponent} from 'react'

// Components
import {Form, Input, InputType, Dropdown, Columns} from 'src/clockface'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'

// Constants
import {DEFAULT_TIME_FORMAT} from 'src/shared/constants'
import {
  FORMAT_OPTIONS,
  TIME_FORMAT_CUSTOM,
  TIME_FORMAT_TOOLTIP_LINK,
} from 'src/dashboards/constants'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  timeFormat: string
  onTimeFormatChange: (format: string) => void
}

interface State {
  customFormat: boolean
  format: string
}

@ErrorHandling
class TimeFormat extends PureComponent<Props, State> {
  public state: State = {
    customFormat: false,
    format: this.props.timeFormat || DEFAULT_TIME_FORMAT,
  }

  public render() {
    const {format, customFormat} = this.state
    const tipContent = `For information on formatting, see <br/><a href="#">${TIME_FORMAT_TOOLTIP_LINK}</a>`

    const formatOption = FORMAT_OPTIONS.find(op => op.text === format)
    const showCustom = !formatOption || customFormat

    return (
      <Form.Element colsXS={Columns.Twelve}>
        <>
          <Form.Label label="Time Format">
            {showCustom && (
              <a href={TIME_FORMAT_TOOLTIP_LINK} target="_blank">
                <QuestionMarkTooltip
                  tipID="Time Axis Format"
                  tipContent={tipContent}
                />
              </a>
            )}
          </Form.Label>
          <Dropdown
            selectedID={showCustom ? TIME_FORMAT_CUSTOM : format}
            onChange={this.handleChooseFormat}
            customClass="dropdown-stretch"
          >
            {FORMAT_OPTIONS.map(({text}) => (
              <Dropdown.Item key={text} id={text} value={text}>
                {text}
              </Dropdown.Item>
            ))}
          </Dropdown>
          {showCustom && (
            <Input
              type={InputType.Text}
              spellCheck={false}
              placeholder="Enter custom format..."
              value={format}
              data-test="custom-time-format"
              customClass="custom-time-format"
              onChange={this.handleChangeFormat}
            />
          )}
        </>
      </Form.Element>
    )
  }

  private get onTimeFormatChange() {
    return this.props.onTimeFormatChange
  }

  private handleChangeFormat = e => {
    const format = e.target.value
    this.onTimeFormatChange(format)
    this.setState({format})
  }

  private handleChooseFormat = (format: string) => {
    if (format === TIME_FORMAT_CUSTOM) {
      this.setState({customFormat: true})
    } else {
      this.onTimeFormatChange(format)
      this.setState({format, customFormat: false})
    }
  }
}

export default TimeFormat
