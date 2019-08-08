// Libraries
import React, {FC, useReducer, ChangeEvent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import RuleSchedule from 'src/alerting/components/notifications/RuleSchedule'
import RuleConditions from 'src/alerting/components/notifications/RuleConditions'
import RuleMessage from 'src/alerting/components/notifications/RuleMessage'
import {
  Panel,
  ComponentSize,
  Overlay,
  Form,
  Input,
  Grid,
  Columns,
} from '@influxdata/clockface'

// Reducers
import {reducer} from './RuleOverlay.reducer'

// Constants
import {endpoints} from 'src/alerting/constants' // Hooks
import {RuleMode, EditRuleDispatch} from 'src/shared/hooks'

// Types
import {NotificationRuleDraft, AppState} from 'src/types'

interface StateProps {
  stateRule: NotificationRuleDraft
}

type Props = WithRouterProps & StateProps

const EditRuleOverlay: FC<Props> = ({params, router, stateRule}) => {
  if (!stateRule) {
    return null
  }

  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  const [rule, dispatch] = useReducer(reducer, stateRule)

  const handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {name, value} = e.target
    dispatch({
      type: 'UPDATE_RULE',
      rule: {...rule, [name]: value} as NotificationRuleDraft,
    })
  }

  return (
    <RuleMode.Provider value="EditRuleDispatch">
      <EditRuleDispatch.Provider value={dispatch}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={800}>
            <Overlay.Header
              title="Edit this Notification Rule"
              onDismiss={handleDismiss}
            />
            <Overlay.Body>
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
            </Overlay.Body>
          </Overlay.Container>
        </Overlay>
      </EditRuleDispatch.Provider>
    </RuleMode.Provider>
  )
}

const mstp = ({rules}: AppState, {params}: Props): StateProps => {
  const stateRule = rules.list.find(r => r.id === params.ruleID)

  return {
    stateRule,
  }
}

export default connect<StateProps>(mstp)(withRouter<Props>(EditRuleOverlay))
