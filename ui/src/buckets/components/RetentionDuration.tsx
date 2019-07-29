// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Form, Input, Grid} from '@influxdata/clockface'

// Utils
import {secondsToDuration} from 'src/utils/formatting'

// Types
import {Columns, InputType} from '@influxdata/clockface'

interface Props {
  type: 'expire'
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
      <Grid.Row>
        <Grid.Column widthSM={Columns.Three}>
          <Form.Element label="Days">
            <Input
              name={DurationKey.Days}
              type={InputType.Number}
              value={`${days}`}
              onChange={onChangeInput}
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthSM={Columns.Three}>
          <Form.Element label="Hours">
            <Input
              name={DurationKey.Hours}
              min={0}
              type={InputType.Number}
              value={`${hours}`}
              onChange={onChangeInput}
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthSM={Columns.Three}>
          <Form.Element label="Minutes">
            <Input
              name={DurationKey.Minutes}
              min={0}
              type={InputType.Number}
              value={`${minutes}`}
              onChange={onChangeInput}
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthSM={Columns.Three}>
          <Form.Element label="Seconds">
            <Input
              name={DurationKey.Seconds}
              min={0}
              type={InputType.Number}
              value={`${seconds}`}
              onChange={onChangeInput}
            />
          </Form.Element>
        </Grid.Column>
      </Grid.Row>
    )
  }
}
