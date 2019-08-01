// Libraries
import React, {FC} from 'react'

// Components
import {Dropdown, ComponentColor} from '@influxdata/clockface'

// Types
import {CheckStatusLevel} from 'src/types'

type Levels = CheckStatusLevel | 'ANY'

const levels: Levels[] = ['CRIT', 'INFO', 'WARN', 'UNKNOWN', 'ANY']

interface Props {
  level: Levels
}

const LevelsDropdown: FC<Props> = ({level}) => {
  const items = levels.map(l => (
    <Dropdown.Item key={l} id={l} value={l}>
      {l}
    </Dropdown.Item>
  ))

  const button = (active, onClick) => (
    <Dropdown.Button
      color={ComponentColor.Primary}
      active={active}
      onClick={onClick}
    >
      {level}
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{items}</Dropdown.Menu>
  )

  return <Dropdown button={button} menu={menu} widthPixels={160} />
}

export default LevelsDropdown
