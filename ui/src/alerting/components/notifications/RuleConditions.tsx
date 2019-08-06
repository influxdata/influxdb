// Libraries
import React, {FC, useContext} from 'react'

// Components
import {
  Grid,
  Columns,
  ComponentSpacer,
  FlexDirection,
  ComponentSize,
  AlignItems,
} from '@influxdata/clockface'
import StatusRuleComponent from 'src/alerting/components/notifications/StatusRule'
import TagRuleComponent from 'src/alerting/components/notifications/TagRule'
import {NewRuleDispatch} from 'src/alerting/components/notifications/NewRuleOverlay'
import DashedButton from 'src/shared/components/dashed_button/DashedButton'

// Constants
import {newTagRule} from 'src/alerting/constants'

// Types
import {RuleState} from './NewRuleOverlay.reducer'

interface Props {
  rule: RuleState
}

const RuleConditions: FC<Props> = ({rule}) => {
  const dispatch = useContext(NewRuleDispatch)
  const {statusRules, tagRules} = rule

  const addTagRule = () => {
    dispatch({
      type: 'ADD_TAG_RULE',
      tagRule: newTagRule,
    })
  }

  const statuses = statusRules.map(status => (
    <StatusRuleComponent key={status.id} status={status} />
  ))

  const tags = tagRules.map(tagRule => (
    <TagRuleComponent key={tagRule.id} tagRule={tagRule} />
  ))

  return (
    <Grid.Row>
      <Grid.Column widthSM={Columns.Two}>Conditions</Grid.Column>
      <Grid.Column widthSM={Columns.Ten}>
        <ComponentSpacer
          direction={FlexDirection.Column}
          margin={ComponentSize.Small}
          alignItems={AlignItems.Stretch}
        >
          {statuses}
          {tags}
          <DashedButton text="+ Tag Rule" onClick={addTagRule} />
        </ComponentSpacer>
      </Grid.Column>
      <Grid.Column>
        <hr />
      </Grid.Column>
    </Grid.Row>
  )
}

export default RuleConditions
