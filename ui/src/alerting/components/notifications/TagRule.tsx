// Libraries
import React, {FC} from 'react'

// Components
import {
  Input,
  Panel,
  DismissButton,
  TextBlock,
  ComponentSpacer,
  ComponentSize,
  FlexDirection,
  ComponentColor,
} from '@influxdata/clockface'

import TagRuleOperatorDropdown, {
  Operator,
} from 'src/alerting/components/notifications/TagRuleOperatorDropdown'

// Utils
import {useRuleDispatch} from './RuleOverlay.reducer'

// Types
import {TagRuleDraft} from 'src/types'

interface Props {
  tagRule: TagRuleDraft
}

const TagRule: FC<Props> = ({tagRule}) => {
  const {key, value, operator} = tagRule.value
  const dispatch = useRuleDispatch()

  const onChange = ({target}) => {
    const {name, value} = target

    const newValue = {
      ...tagRule.value,
      [name]: value,
    }

    dispatch({
      type: 'UPDATE_TAG_RULES',
      tagRule: {
        ...tagRule,
        value: newValue,
      },
    })
  }

  const onSelectOperator = (operator: Operator) => {
    dispatch({
      type: 'SET_TAG_RULE_OPERATOR',
      tagRuleID: tagRule.cid,
      operator,
    })
  }

  const onDelete = () => {
    dispatch({
      type: 'DELETE_TAG_RULE',
      tagRuleID: tagRule.cid,
    })
  }

  return (
    <Panel testID="tag-rule" size={ComponentSize.ExtraSmall}>
      <DismissButton onClick={onDelete} color={ComponentColor.Default} />
      <Panel.Body>
        <ComponentSpacer
          direction={FlexDirection.Row}
          margin={ComponentSize.Small}
        >
          <TextBlock text="When tag" />
          <ComponentSpacer.FlexChild grow={1}>
            <Input
              testID="tag-rule-key--input"
              placeholder="Key"
              value={key}
              name="key"
              onChange={onChange}
            />
          </ComponentSpacer.FlexChild>
          <ComponentSpacer.FlexChild grow={0} basis={60}>
            <TagRuleOperatorDropdown
              selectedOperator={operator}
              onSelect={onSelectOperator}
            />
          </ComponentSpacer.FlexChild>
          <ComponentSpacer.FlexChild grow={1}>
            <Input
              testID="tag-rule-key--input"
              placeholder="Value"
              value={value}
              name="value"
              onChange={onChange}
            />
          </ComponentSpacer.FlexChild>
        </ComponentSpacer>
      </Panel.Body>
    </Panel>
  )
}

export default TagRule
