import React, {PureComponent} from 'react'

// Components
import {Form, Input, ComponentSpacer, Grid} from '@influxdata/clockface'
import {Dropdown} from 'src/clockface'

// Constants
import {DEFAULT_TIME_FORMAT} from 'src/shared/constants'
import {
  FORMAT_OPTIONS,
  TIME_FORMAT_CUSTOM,
  TIME_FORMAT_TOOLTIP_LINK,
} from 'src/dashboards/constants'

// Types
import {
  Columns,
  InputType,
  FlexDirection,
  AlignItems,
} from '@influxdata/clockface'

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
    const {format} = this.state

    return (
      <Grid.Column widthSM={Columns.Twelve}>
        <Form.Element label="Time Format">
          <ComponentSpacer
            direction={FlexDirection.Column}
            alignItems={AlignItems.FlexStart}
          >
            <Dropdown
              selectedID={this.showCustom ? TIME_FORMAT_CUSTOM : format}
              onChange={this.handleChooseFormat}
              customClass="dropdown-stretch"
            >
              {FORMAT_OPTIONS.map(({text}) => (
                <Dropdown.Item key={text} id={text} value={text}>
                  {text}
                </Dropdown.Item>
              ))}
            </Dropdown>
            {this.customTimeInput}
            {this.helpBox}
          </ComponentSpacer>
        </Form.Element>
      </Grid.Column>
    )
  }

  private get showCustom(): boolean {
    const {format, customFormat} = this.state

    const formatOption = FORMAT_OPTIONS.find(op => op.text === format)
    return !formatOption || customFormat
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

  private get customTimeInput(): JSX.Element {
    const {format} = this.state

    if (this.showCustom) {
      return (
        <Input
          type={InputType.Text}
          spellCheck={false}
          placeholder="Enter custom format..."
          value={format}
          data-testid="custom-time-format"
          customClass="custom-time-format"
          onChange={this.handleChangeFormat}
        />
      )
    }
  }

  private get helpBox(): JSX.Element {
    if (this.showCustom) {
      return (
        <Form.Box>
          <p className="time-format--helper">
            For help with formatting time, see{' '}
            <a href={TIME_FORMAT_TOOLTIP_LINK} target="_blank">
              MomentJS Docs
            </a>
          </p>
        </Form.Box>
      )
    }
  }
}

export default TimeFormat
