// Libraries
import React, {FC} from 'react'

// Components
import {SquareButton, IconFont, ComponentStatus} from '@influxdata/clockface'

interface Props {
  onMoveUp?: () => void
}

const MovePanelUpButton: FC<Props> = ({onMoveUp}) => {
  const status = onMoveUp ? ComponentStatus.Default : ComponentStatus.Disabled

  const handleClick = (): void => {
    if (onMoveUp) {
      onMoveUp()
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
