// Libraries
import React, {FC, useContext} from 'react'
import {v4} from 'uuid'

// Components
import {
  Grid,
  Columns,
  Button,
  ComponentSpacer,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'
import StatusRuleComponent from 'src/alerting/components/notifications/StatusRule'
import {NewRuleDispatch} from 'src/alerting/components/notifications/NewRuleOverlay'

// Constants
import {newStatusRule, newTagRule} from 'src/alerting/constants'

// Types
import {RuleState} from './NewRuleOverlay.reducer'

interface Props {
  rule: RuleState
}

const RuleConditions: FC<Props> = ({rule}) => {
  const dispatch = useContext(NewRuleDispatch)
  const {statusRules, tagRules} = rule
  const addStatusRule = () => {
    dispatch({
      type: 'ADD_STATUS_RULE',
      statusRule: newStatusRule,
    })
  }

  const addTagRule = () => {
    dispatch({
      type: 'ADD_TAG_RULE',
      tagRule: newTagRule,
    })
  }

  const statuses = statusRules.map(status => (
    <StatusRuleComponent key={status.id} status={status} />
  ))

  const tags = tagRules.map(_ => <div key={v4()}>im a tag rule</div>)

  return (
    <>
      <h2 className="cf-overlay--title">Rule Conditions</h2>
      <Grid.Row>
        <Grid.Column widthSM={Columns.Three}>
          <ComponentSpacer
            direction={FlexDirection.Row}
            justifyContent={JustifyContent.SpaceBetween}
          >
            <Button
              text="+ Status Rule"
              onClick={addStatusRule}
              testID="add-status-rule--button"
            />
            <Button text="+ Tag Rule" onClick={addTagRule} />
          </ComponentSpacer>
        </Grid.Column>
      </Grid.Row>
      <Grid.Row>{statuses}</Grid.Row>
      <Grid.Row>{tags}</Grid.Row>
    </>
  )
}

export default RuleConditions
