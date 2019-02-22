// Libraries
import React, {PureComponent} from 'react'
import ReactDatePicker from 'react-datepicker'

// Styles
import 'react-datepicker/dist/react-datepicker.css'
import {Input, Form, Grid} from 'src/clockface'
import {ComponentSize, Columns} from '@influxdata/clockface'

interface Props {
  label: string
  dateTime: string
  onSelectDate: (date: string) => void
}

class DatePicker extends PureComponent<Props> {
  private inCurrentMonth: boolean = false

  public render() {
    const {dateTime, label} = this.props
    const date = new Date(dateTime)

    return (
      <div className="range-picker--date-picker">
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <Form.Element label={label}>
              <ReactDatePicker
                selected={date}
                onChange={this.handleSelectDate}
                startOpen={true}
                dateFormat="yyyy-MM-dd HH:mm"
                showTimeSelect={true}
                timeFormat="HH:mm"
                shouldCloseOnSelect={false}
                disabledKeyboardNavigation={true}
                customInput={this.customInput}
                popperContainer={this.popperContainer}
                popperClassName="range-picker--popper"
                calendarClassName="range-picker--calendar"
                dayClassName={this.dayClassName}
                timeIntervals={60}
                fixedHeight={true}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </div>
    )
  }

  private get customInput() {
    const {label} = this.props

    return (
      <Input
        size={ComponentSize.Medium}
        customClass="range-picker--input react-datepicker-ignore-onclickoutside"
        titleText={label}
      />
    )
  }

  private dayClassName = (date: Date) => {
    const day = date.getDate()

    if (day === 1) {
      this.inCurrentMonth = !this.inCurrentMonth
    }

    if (this.inCurrentMonth) {
      return 'range-picker--day-in-month'
    }

    return 'range-picker--day'
  }

  private popperContainer({children}): JSX.Element {
    return <div className="range-picker--popper-container">{children}</div>
  }

  private handleSelectDate = (date: Date): void => {
    const {onSelectDate} = this.props

    onSelectDate(date.toISOString())
  }
}

export default DatePicker
