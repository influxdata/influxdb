// Libraries
import React, {FC} from 'react'

// Components
import {
  Input,
  Panel,
  DismissButton,
  TextBlock,
  FlexBox,
  ComponentSize,
  FlexDirection,
  ComponentColor,
  InfluxColors,
} from '@influxdata/clockface'

import TagRuleOperatorDropdown, {
  Operator,
} from 'src/notifications/rules/components/TagRuleOperatorDropdown'

// Utils
import {useRuleDispatch} from './RuleOverlayProvider'

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
    <Panel testID="tag-rule">
      <DismissButton onClick={onDelete} color={ComponentColor.Default} />
      <Panel.Body size={ComponentSize.ExtraSmall}>
        <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Small}>
          <TextBlock
            text="AND"
            textColor={InfluxColors.Honeydew}
            backgroundColor={InfluxColors.Pepper}
          />
          <TextBlock text="When" />
          <FlexBox.Child grow={1}>
            <Input
              testID="tag-rule-key--input"
              placeholder="Tag"
              value={key}
              name="key"
              onChange={onChange}
            />
          </FlexBox.Child>
          <FlexBox.Child grow={0} basis={60}>
            <TagRuleOperatorDropdown
              selectedOperator={operator}
              onSelect={onSelectOperator}
            />
          </FlexBox.Child>
          <FlexBox.Child grow={1}>
            <Input
              testID="tag-rule-key--input"
              placeholder="Value"
              value={value}
              name="value"
              onChange={onChange}
            />
          </FlexBox.Child>
        </FlexBox>
      </Panel.Body>
    </Panel>
  )
}

export default TagRule
