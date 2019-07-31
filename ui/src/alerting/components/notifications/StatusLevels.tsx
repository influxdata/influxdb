// Libraries
import React, {FC} from 'react'

// Types
import {StatusRuleItem} from 'src/types'
import {Dropdown} from '@influxdata/clockface'

interface Props {
  status: StatusRuleItem
}

const changes = ['changes from', 'equals']
const activeChange = status => {
  if (!!status.value.previousLevel) {
    return 'equals'
  }

  return 'changes from'
}

const StatusLevels: FC<Props> = ({status}) => {
  const items = changes.map(change => (
    <Dropdown.Item key={change} id={change} value={change}>
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

  return (
    <div className="status-levels--container">
      <div className="sentence-frag">When status</div>
      <Dropdown button={button} menu={menu} widthPixels={160} />
    </div>
  )
}

export default StatusLevels
