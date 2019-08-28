// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Form, Input, InputType, Grid, Columns} from '@influxdata/clockface'

// Types
import {RuleState} from './RuleOverlay.reducer'

interface Props {
  rule: RuleState
  onChange: (e: ChangeEvent) => void
}

const RuleSchedule: FC<Props> = ({rule, onChange}) => {
  const {every, offset} = rule

  return (
    <Grid.Row>
      <Grid.Column widthXS={Columns.Four}>
        <Form.Element label="Schedule Every">
          <Input
            value={every}
            name="every"
            type={InputType.Text}
            placeholder="1d3h30s"
            onChange={onChange}
            testID="rule-schedule-every--input"
          />
        </Form.Element>
      </Grid.Column>

      <Grid.Column widthXS={Columns.Four}>
        <Form.Element label="Offset">
          <Input
            name="offset"
            type={InputType.Text}
            value={offset}
            placeholder="20m"
            onChange={onChange}
            testID="rule-schedule-offset--input"
          />
        </Form.Element>
      </Grid.Column>
    </Grid.Row>
  )
}

export default RuleSchedule
