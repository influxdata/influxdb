// Libraries
import React, {FC} from 'react'

// Components
import {Dropdown, ComponentColor} from '@influxdata/clockface'

// Types
import {CheckStatusLevel} from 'src/types'

type Levels = CheckStatusLevel
type levelType = 'currentLevel' | 'previousLevel'

const levels: Levels[] = ['CRIT', 'INFO', 'WARN', 'UNKNOWN']

interface Props {
  level: Levels
  type: levelType
  onClickLevel: (type: levelType, level: Levels) => void
}

const LevelsDropdown: FC<Props> = ({type, level, onClickLevel}) => {
  const items = levels.map(l => (
    <Dropdown.Item
      key={l}
      id={l}
      value={l}
      onClick={() => onClickLevel(type, l)}
    >
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
