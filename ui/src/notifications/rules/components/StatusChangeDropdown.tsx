// Libraries
import React, {FC} from 'react'

// Types
import {StatusRuleDraft} from 'src/types'

// Components
import {Dropdown} from '@influxdata/clockface'

// Utils
import {useRuleDispatch} from './RuleOverlayProvider'
import {
  CHANGES,
  changeStatusRule,
  activeChange,
} from 'src/notifications/rules/utils'

interface Props {
  status: StatusRuleDraft
}

const StatusChangeDropdown: FC<Props> = ({status}) => {
  const dispatch = useRuleDispatch()

  const statusChange = (s, c) =>
    dispatch({
      type: 'UPDATE_STATUS_RULES',
      statusRule: changeStatusRule(s, c),
    })

  const items = CHANGES.map(change => (
    <Dropdown.Item
      key={change}
      id={change}
      value={change}
      testID={`status-change--dropdown-item ${change}`}
      onClick={() => statusChange(status, change)}
    >
      {change}
    </Dropdown.Item>
  ))

  const buttonText = activeChange(status)

  const button = (active, onClick) => (
    <Dropdown.Button
      testID="status-change--dropdown--button"
      active={active}
      onClick={onClick}
    >
      {buttonText}
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{items}</Dropdown.Menu>
  )

  return (
    <Dropdown button={button} menu={menu} testID="status-change--dropdown" />
  )
}

export default StatusChangeDropdown
