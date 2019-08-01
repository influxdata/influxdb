// Libraries
import React, {FC, Dispatch} from 'react'
import {omit} from 'lodash'

// Types
import {StatusRuleItem} from 'src/types'
import {Actions} from 'src/alerting/components/notifications/NewRuleOverlay.reducer'

// Components
import {Dropdown} from '@influxdata/clockface'

interface Props {
  status: StatusRuleItem
  dispatch: Dispatch<Actions>
}

const previousLevel = {level: 'OK'}
const changes = ['changes from', 'equals']

const activeChange = status => {
  if (!!status.value.previousLevel) {
    return 'changes from'
  }

  return 'equals'
}

const changeStatusRule = (status, change) => {
  if (change === 'equals' && status.value.previousLevel) {
    return omit(status, 'value.previousLevel')
  }

  const {value} = status
  const newValue = {...value, previousLevel}

  return {...status, value: newValue}
}

const StatusChangeDropdown: FC<Props> = ({status, dispatch}) => {
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
