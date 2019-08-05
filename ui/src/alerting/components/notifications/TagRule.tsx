// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Input, Form} from '@influxdata/clockface'
import {NewRuleDispatch} from 'src/alerting/components/notifications/NewRuleOverlay'
import TagRuleOperatorDropdown, {
  Operator,
} from 'src/alerting/components/notifications/TagRuleOperatorDropdown'

// Types
import {TagRuleItem} from 'src/types'

interface Props {
  tagRule: TagRuleItem
}

const TagRule: FC<Props> = ({tagRule}) => {
  const {key, value, operator} = tagRule.value
  const dispatch = useContext(NewRuleDispatch)

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
      type: 'UPDATE_TAG_RULES',
      tagRule: {
        ...tagRule,
        value: {
          ...tagRule.value,
          operator,
        },
      },
    })
  }

  return (
    <div className="condition-row tag-rule" data-testid="tag-rule">
      <Form.Element label="Key">
        <Input
          testID="tag-rule-key--input"
          placeholder="Key"
          value={key}
          name="key"
          onChange={onChange}
        />
      </Form.Element>
      <TagRuleOperatorDropdown
        selectedOperator={operator}
        onSelect={onSelectOperator}
      />
      <Form.Element label="Value">
        <Input
          testID="tag-rule-key--input"
          placeholder="Value"
          value={value}
          name="value"
          onChange={onChange}
        />
      </Form.Element>
    </div>
  )
}

export default TagRule
