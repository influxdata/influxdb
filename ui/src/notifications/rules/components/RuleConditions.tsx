// Libraries
import React, {FC} from 'react'

// Components
import {
  Grid,
  Columns,
  FlexBox,
  FlexDirection,
  ComponentSize,
  ComponentColor,
  AlignItems,
} from '@influxdata/clockface'
import StatusRuleComponent from 'src/notifications/rules/components/StatusRule'
import TagRuleComponent from 'src/notifications/rules/components/TagRule'
import DashedButton from 'src/shared/components/dashed_button/DashedButton'

// Utils
import {useRuleDispatch} from './RuleOverlayProvider'

// Constants
import {newTagRuleDraft} from 'src/notifications/rules/utils'

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
      tagRule: newTagRuleDraft(),
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
        <FlexBox
          direction={FlexDirection.Column}
          margin={ComponentSize.Small}
          alignItems={AlignItems.Stretch}
        >
          {statuses}
          {tags}
          <DashedButton
            text="+ Tag Filter"
            onClick={addTagRule}
            color={ComponentColor.Primary}
            size={ComponentSize.Small}
          />
        </FlexBox>
      </Grid.Column>
      <Grid.Column>
        <hr />
      </Grid.Column>
    </Grid.Row>
  )
}

export default RuleConditions
