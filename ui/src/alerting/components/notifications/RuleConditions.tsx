// Libraries
import React, {FC} from 'react'

// Components
import {
  Grid,
  Columns,
  ComponentSpacer,
  FlexDirection,
  ComponentSize,
  ComponentColor,
  AlignItems,
} from '@influxdata/clockface'
import StatusRuleComponent from 'src/alerting/components/notifications/StatusRule'
import TagRuleComponent from 'src/alerting/components/notifications/TagRule'
import DashedButton from 'src/shared/components/dashed_button/DashedButton'

// Utils
import {useRuleDispatch} from './RuleOverlay.reducer'

// Constants
import {NEW_TAG_RULE_DRAFT} from 'src/alerting/constants'

// Types
import {RuleState} from './RuleOverlay.reducer'

interface Props {
  rule: RuleState
}

const RuleConditions: FC<Props> = ({rule}) => {
  const dispatch = useRuleDispatch()
  const {statusRules, tagRules} = rule

  const addTagRule = () => {
    dispatch({
      type: 'ADD_TAG_RULE',
      tagRule: NEW_TAG_RULE_DRAFT,
    })
  }

  const statuses = statusRules.map(status => (
    <StatusRuleComponent key={status.cid} status={status} />
  ))

  const tags = tagRules.map(tagRule => (
    <TagRuleComponent key={tagRule.cid} tagRule={tagRule} />
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
          <DashedButton
            text="+ Tag Rule"
            onClick={addTagRule}
            color={ComponentColor.Primary}
            size={ComponentSize.Small}
          />
        </ComponentSpacer>
      </Grid.Column>
      <Grid.Column>
        <hr />
      </Grid.Column>
    </Grid.Row>
  )
}

export default RuleConditions
