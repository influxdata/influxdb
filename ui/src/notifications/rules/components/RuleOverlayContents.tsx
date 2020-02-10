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
import RuleSchedule from 'src/notifications/rules/components/RuleSchedule'
import RuleConditions from 'src/notifications/rules/components/RuleConditions'
import RuleMessage from 'src/notifications/rules/components/RuleMessage'
import RuleOverlayFooter from 'src/notifications/rules/components/RuleOverlayFooter'

// Utils
import {useRuleState, useRuleDispatch} from './RuleOverlayProvider'

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

  const handleChangeParameter = (key: keyof NotificationRuleDraft) => (
    value: string
  ) => {
    dispatch({
      type: 'UPDATE_RULE',
      rule: {...rule, [key]: value} as NotificationRuleDraft,
    })
  }

  return (
    <Grid>
      <Form>
        <Grid.Row>
          <Grid.Column widthSM={Columns.Two}>About</Grid.Column>
          <Grid.Column widthSM={Columns.Ten}>
            <Panel>
              <Panel.Body size={ComponentSize.ExtraSmall}>
                <Form.Element label="Name">
                  <Input
                    testID="rule-name--input"
                    placeholder="Name this new rule"
                    value={rule.name}
                    name="name"
                    onChange={handleChange}
                  />
                </Form.Element>
                <RuleSchedule rule={rule} onChange={handleChangeParameter} />
              </Panel.Body>
            </Panel>
          </Grid.Column>
          <Grid.Column>
            <hr />
          </Grid.Column>
        </Grid.Row>
        <RuleConditions rule={rule} />
        <RuleMessage rule={rule} />
        <RuleOverlayFooter saveButtonText={saveButtonText} onSave={onSave} />
      </Form>
    </Grid>
  )
}

export default RuleOverlayContents
