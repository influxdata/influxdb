// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Form, Input, InputType, Columns} from 'src/clockface'

// Utils
import {Duration, durationToMs, msToDuration} from 'src/utils/formatting'

interface Props {
  retentionPeriod: number
  onChangeRetentionPeriod: (rp: number) => void
}

enum DurationKey {
  Days = 'days',
  Hours = 'hours',
  Minutes = 'minutes',
  Seconds = 'seconds',
}

interface State {
  duration: Duration
}

export default class RetentionPeriod extends PureComponent<Props, State> {
  public render() {
    const {retentionPeriod} = this.props
    const {days, hours, minutes, seconds} = msToDuration(retentionPeriod)

    return (
      <>
        <Form.Element label="Days" colsXS={Columns.Three}>
          <Input
            name={DurationKey.Days}
            type={InputType.Number}
            value={`${days}`}
            onChange={this.handleChangeInput}
          />
        </Form.Element>
        <Form.Element label="Hours" colsXS={Columns.Three}>
          <Input
            name={DurationKey.Hours}
            min="0"
            type={InputType.Number}
            value={`${hours}`}
            onChange={this.handleChangeInput}
          />
        </Form.Element>
        <Form.Element label="Minutes" colsXS={Columns.Three}>
          <Input
            name={DurationKey.Minutes}
            min="0"
            type={InputType.Number}
            value={`${minutes}`}
            onChange={this.handleChangeInput}
          />
        </Form.Element>
        <Form.Element label="Seconds" colsXS={Columns.Three}>
          <Input
            name={DurationKey.Seconds}
            min="0"
            type={InputType.Number}
            value={`${seconds}`}
            onChange={this.handleChangeInput}
          />
        </Form.Element>
      </>
    )
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const {retentionPeriod} = this.props
    const value = e.target.value
    const key = e.target.name as keyof Duration
    const time = {...msToDuration(retentionPeriod), [key]: Number(value)}
    const ms = durationToMs(time)

    this.props.onChangeRetentionPeriod(ms)
  }
}
