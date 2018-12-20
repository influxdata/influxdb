import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Form, Input, InputType, Columns} from 'src/clockface'

// Utils
import {secondsToDuration} from 'src/utils/formatting'
import {BucketRetentionRules} from 'src/api'

interface Props {
  type: BucketRetentionRules.TypeEnum
  retentionSeconds: number
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
}

enum DurationKey {
  Days = 'days',
  Hours = 'hours',
  Minutes = 'minutes',
  Seconds = 'seconds',
}

export default class RetentionDuration extends PureComponent<Props> {
  public render() {
    const {type, retentionSeconds, onChangeInput} = this.props
    const {days, hours, minutes, seconds} = secondsToDuration(retentionSeconds)

    if (type === null) {
      return null
    }

    return (
      <>
        <Form.Element label="Days" colsXS={Columns.Two}>
          <Input
            name={DurationKey.Days}
            type={InputType.Number}
            value={`${days}`}
            onChange={onChangeInput}
          />
        </Form.Element>
        <Form.Element label="Hours" colsXS={Columns.Two}>
          <Input
            name={DurationKey.Hours}
            min={0}
            type={InputType.Number}
            value={`${hours}`}
            onChange={onChangeInput}
          />
        </Form.Element>
        <Form.Element label="Minutes" colsXS={Columns.Two}>
          <Input
            name={DurationKey.Minutes}
            min={0}
            type={InputType.Number}
            value={`${minutes}`}
            onChange={onChangeInput}
          />
        </Form.Element>
        <Form.Element label="Seconds" colsXS={Columns.Two}>
          <Input
            name={DurationKey.Seconds}
            min={0}
            type={InputType.Number}
            value={`${seconds}`}
            onChange={onChangeInput}
          />
        </Form.Element>
      </>
    )
  }
}
