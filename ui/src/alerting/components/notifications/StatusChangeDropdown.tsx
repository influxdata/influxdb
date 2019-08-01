// Libraries
import React, {FC, useContext} from 'react'

// Types
import {StatusRuleItem} from 'src/types'

// Components
import {Dropdown} from '@influxdata/clockface'
import {NewRuleDispatch} from 'src/alerting/components/notifications/NewRuleOverlay'

// Utils
import {changes, changeStatusRule, activeChange} from './statusChange'

interface Props {
  status: StatusRuleItem
}

const StatusChangeDropdown: FC<Props> = ({status}) => {
  const dispatch = useContext(NewRuleDispatch)

  const statusChange = (s, c) =>
    dispatch({
      type: 'UPDATE_STATUS_RULES',
      status: changeStatusRule(s, c),
    })

  const items = changes.map(change => (
    <Dropdown.Item
      key={change}
      id={change}
      value={change}
      onClick={() => statusChange(status, change)}
    >
      {change}
    </Dropdown.Item>
  ))

  const button = (active, onClick) => (
    <Dropdown.Button active={active} onClick={onClick}>
      {activeChange(status)}
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{items}</Dropdown.Menu>
  )

  return <Dropdown button={button} menu={menu} widthPixels={160} />
}

export default StatusChangeDropdown
