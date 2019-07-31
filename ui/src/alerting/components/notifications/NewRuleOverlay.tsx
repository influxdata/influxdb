// Libraries
import React, {FC, useReducer} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import RuleSchedule from 'src/alerting/components/notifications/RuleSchedule'
import RuleConditions from 'src/alerting/components/notifications/RuleConditions'
import {Overlay, Form, Input, Grid} from '@influxdata/clockface'

// Reducers
import {reducer, RuleState} from './NewRuleOverlay.reducer'

// Constants
import {newRule} from 'src/alerting/constants'

// Types
import {NotificationRuleUI} from 'src/types'

type Props = WithRouterProps

export const newRuleState: RuleState = {
  ...newRule,
  schedule: 'every',
}

const NewRuleOverlay: FC<Props> = ({params, router}) => {
  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  const [rule, dispatch] = useReducer(reducer, newRuleState)

  const handleChange = e => {
    const {name, value} = e.target
    dispatch({
      type: 'UPDATE_RULE',
      rule: {...rule, [name]: value} as NotificationRuleUI,
    })
  }

  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={Infinity} className="rule-eo">
        <Overlay.Header
          title="Create a Notification Rule"
          onDismiss={handleDismiss}
        />
        <Overlay.Body>
          <Grid>
            <Form>
              <Grid.Row>
                <Grid.Column>
                  <Form.Element label="Name">
                    <Input
                      placeholder="Name this new rule"
                      value={rule.name}
                      name="name"
                      onChange={handleChange}
                    />
                  </Form.Element>
                </Grid.Column>
              </Grid.Row>
              <RuleSchedule
                rule={rule}
                onChange={handleChange}
                dispatch={dispatch}
              />
              <RuleConditions rule={rule} dispatch={dispatch} />
            </Form>
          </Grid>
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

export default withRouter<Props>(NewRuleOverlay)
