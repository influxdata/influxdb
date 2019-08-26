// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {
  Radio,
  Form,
  Input,
  InputType,
  Grid,
  Columns,
  ButtonShape,
} from '@influxdata/clockface'

// Utils
import {useRuleDispatch} from './RuleOverlayProvider'

// Types
import {RuleState} from './RuleOverlay.reducer'

interface Props {
  rule: RuleState
  onChange: (e: ChangeEvent) => void
}

const RuleSchedule: FC<Props> = ({rule, onChange}) => {
  const {cron, every, offset} = rule
  const dispatch = useRuleDispatch()

  const isEvery = rule.hasOwnProperty('every')
  const label = isEvery ? 'Every' : 'Cron'
  const name = isEvery ? 'every' : 'cron'
  const placeholder = isEvery ? '1d3h30s' : '0 2 * * *'
  const value = isEvery ? every : cron

  return (
    <Grid.Row>
      <Grid.Column widthXS={Columns.Four}>
        <Form.Element label="Schedule">
          <Radio shape={ButtonShape.StretchToFit}>
            <Radio.Button
              id="every"
              testID="rule-schedule-every"
              active={isEvery}
              value="every"
              titleText="Run task at regular intervals"
              onClick={() =>
                dispatch({
                  type: 'SET_ACTIVE_SCHEDULE',
                  schedule: 'every',
                })
              }
            >
              Every
            </Radio.Button>
            <Radio.Button
              id="cron"
              testID="rule-schedule-cron"
              active={!isEvery}
              value="cron"
              titleText="Use cron syntax for more control over scheduling"
              onClick={() =>
                dispatch({
                  type: 'SET_ACTIVE_SCHEDULE',
                  schedule: 'cron',
                })
              }
            >
              Cron
            </Radio.Button>
          </Radio>
        </Form.Element>
      </Grid.Column>
      <Grid.Column widthXS={Columns.Four}>
        <Form.Element label={label}>
          <Input
            value={value}
            name={name}
            type={InputType.Text}
            placeholder={placeholder}
            onChange={onChange}
            testID={`rule-schedule-${name}--input`}
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
