// Libraries
import React, {FC} from 'react'

// Components
import {Dropdown} from '@influxdata/clockface'

// Types
import {TagRuleDraft} from 'src/types'

interface Props {
  selectedOperator: Operator
  onSelect: (operator: Operator) => void
}

export type Operator = TagRuleDraft['value']['operator']

const operators: {operator: Operator; display: string}[] = [
  {operator: 'equal', display: '=='},
]

const TagRuleOperatorDropdown: FC<Props> = ({selectedOperator, onSelect}) => {
  const items = operators.map(({operator, display}) => (
    <Dropdown.Item
      key={operator}
      id={operator}
      value={display}
      testID={`tag-rule--dropdown-item ${operator}`}
      onClick={() => onSelect(operator)}
    >
      {display}
    </Dropdown.Item>
  ))

  const buttonText = operators.find(o => o.operator === selectedOperator)

  const button = (active, onClick) => (
    <Dropdown.Button
      testID="tag-rule--dropdown--button"
      active={active}
      onClick={onClick}
    >
      {buttonText.display}
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{items}</Dropdown.Menu>
  )

  return <Dropdown menu={menu} button={button} testID="tag-rule--dropdown" />
}

export default TagRuleOperatorDropdown
