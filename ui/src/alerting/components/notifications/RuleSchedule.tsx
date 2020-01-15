// Libraries
import React, {FC} from 'react'

// Components
import {Form, Grid, Columns} from '@influxdata/clockface'
import DurationInput from 'src/shared/components/DurationInput'

// Types
import {RuleState} from './RuleOverlay.reducer'
import {DURATIONS} from 'src/timeMachine/constants/queryBuilder'
import {NotificationRuleDraft} from 'src/types'
import {CHECK_OFFSET_OPTIONS} from 'src/alerting/constants'

interface Props {
  rule: RuleState
  onChange: (key: keyof NotificationRuleDraft) => (value: string) => void
}

const RuleSchedule: FC<Props> = ({rule, onChange}) => {
  const {every, offset} = rule

  return (
    <Grid.Row>
      <Grid.Column widthXS={Columns.Four}>
        <Form.Element label="Schedule Every">
          <DurationInput
            value={every || ''}
            onSubmit={onChange('every')}
            suggestions={DURATIONS}
            placeholder="1d3h30s"
            testID="rule-schedule-every--input"
          />
        </Form.Element>
      </Grid.Column>

      <Grid.Column widthXS={Columns.Four}>
        <Form.Element label="Offset">
          <DurationInput
            value={offset || ''}
            onSubmit={onChange('offset')}
            suggestions={CHECK_OFFSET_OPTIONS}
            placeholder="10m"
            testID="rule-schedule-offset--input"
          />
        </Form.Element>
      </Grid.Column>
    </Grid.Row>
  )
}

export default RuleSchedule
