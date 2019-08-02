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
  selectedLevel: Levels
  type: levelType
  onClickLevel: (type: levelType, level: Levels) => void
}

const LevelsDropdown: FC<Props> = ({type, selectedLevel, onClickLevel}) => {
  const button = (active, onClick) => (
    <Dropdown.Button
      color={ComponentColor.Primary}
      active={active}
      onClick={onClick}
      testID={`levels--dropdown--button ${type}`}
    >
      {selectedLevel}
    </Dropdown.Button>
  )

  const items = levels.map(level => (
    <Dropdown.Item
      key={level}
      id={level}
      value={level}
      onClick={() => onClickLevel(type, level)}
      testID={`levels--dropdown-item ${level}`}
    >
      {level}
    </Dropdown.Item>
  ))

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{items}</Dropdown.Menu>
  )

  return (
    <Dropdown
      button={button}
      menu={menu}
      widthPixels={160}
      testID={`levels--dropdown ${type}`}
    />
  )
}

export default LevelsDropdown
