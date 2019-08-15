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
import RuleOverlayFooter from 'src/alerting/components/notifications/RuleOverlayFooter'

// Utils
import {useRuleState, useRuleDispatch} from './RuleOverlay.reducer'

// Types
import {NotificationRuleDraft} from 'src/types'

interface Props {
  saveButtonText: string
  onSave: (draftRule: NotificationRuleDraft) => Promise<void>
}

const RuleOverlayContents: FC<Props> = ({saveButtonText, onSave}) => {
  const rule = useRuleState()
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
        {/* TODO: Connect to Redux endpoints once they exist */}
        <RuleMessage rule={rule} endpoints={[]} />
        <RuleOverlayFooter saveButtonText={saveButtonText} onSave={onSave} />
      </Form>
    </Grid>
  )
}

export default RuleOverlayContents
