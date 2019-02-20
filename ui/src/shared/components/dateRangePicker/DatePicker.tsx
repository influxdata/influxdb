// Libraries
import React, {PureComponent} from 'react'
import ReactDatePicker from 'react-datepicker'

// Styles
import 'react-datepicker/dist/react-datepicker.css'
import {Input} from 'src/clockface'
import {ComponentSize} from '@influxdata/clockface'
import FormLabel from 'src/clockface/components/form_layout/FormLabel'

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
      <FormLabel label={label}>
        <div className="range-picker--date-picker">
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
        </div>
      </FormLabel>
    )
  }

  private get customInput() {
    return (
      <Input
        widthPixels={314}
        size={ComponentSize.Medium}
        customClass="range-picker--input react-datepicker-ignore-onclickoutside"
        titleText="Start"
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
