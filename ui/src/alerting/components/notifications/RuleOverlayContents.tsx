// Libraries
import React, {FC} from 'react'

// Components
import {
  Grid,
  Panel,
  Columns,
  ComponentSize,
  Form,
  Input,
} from '@influxdata/clockface'
import RuleSchedule from 'src/alerting/components/notifications/RuleSchedule'
import RuleConditions from 'src/alerting/components/notifications/RuleConditions'
import RuleMessage from 'src/alerting/components/notifications/RuleMessage'

// Types
import {NotificationRuleDraft} from 'src/types'

// Constants
import {endpoints} from 'src/alerting/constants'

// Hooks
import {useRuleDispatch} from 'src/shared/hooks'

interface Props {
  rule: NotificationRuleDraft
}

const RuleOverlayContents: FC<Props> = ({rule}) => {
  const dispatch = useRuleDispatch()
  const handleChange = e => {
    const {name, value} = e.target
    dispatch({
      type: 'UPDATE_RULE',
      rule: {...rule, [name]: value} as NotificationRuleDraft,
    })
  }

  return (
    <Grid>
      <Form>
        <Grid.Row>
          <Grid.Column widthSM={Columns.Two}>About</Grid.Column>
          <Grid.Column widthSM={Columns.Ten}>
            <Panel size={ComponentSize.ExtraSmall}>
              <Panel.Body>
                <Form.Element label="Name">
                  <Input
                    testID="rule-name--input"
                    placeholder="Name this new rule"
                    value={rule.name}
                    name="name"
                    onChange={handleChange}
                  />
                </Form.Element>
                <RuleSchedule rule={rule} onChange={handleChange} />
              </Panel.Body>
            </Panel>
          </Grid.Column>
          <Grid.Column>
            <hr />
          </Grid.Column>
        </Grid.Row>
        <RuleConditions rule={rule} />
        <RuleMessage rule={rule} endpoints={endpoints} />
      </Form>
    </Grid>
  )
}

export default RuleOverlayContents
