// Libraries
import React, {FC} from 'react'

// Components
import {SquareButton, IconFont, ComponentStatus} from '@influxdata/clockface'

interface Props {
  id: string
  onMoveDown?: (id: string) => void
}

const MovePanelDownButton: FC<Props> = ({id, onMoveDown}) => {
  const status = onMoveDown ? ComponentStatus.Default : ComponentStatus.Disabled

  const handleClick = (): void => {
    if (onMoveDown) {
      onMoveDown(id)
    }
  }

  return (
    <SquareButton
      icon={IconFont.CaretDown}
      onClick={handleClick}
      status={status}
    />
  )
}

export default MovePanelDownButton
