// Libraries
import React, {FC} from 'react'

// Components
import {SquareButton, IconFont, ComponentStatus} from '@influxdata/clockface'

interface Props {
  id: string
  onMoveUp?: (id: string) => void
}

const MovePanelUpButton: FC<Props> = ({id, onMoveUp}) => {
  const status = onMoveUp ? ComponentStatus.Default : ComponentStatus.Disabled

  const handleClick = (): void => {
    if (onMoveUp) {
      onMoveUp(id)
    }
  }

  return (
    <SquareButton
      icon={IconFont.CaretUp}
      onClick={handleClick}
      status={status}
    />
  )
}

export default MovePanelUpButton
