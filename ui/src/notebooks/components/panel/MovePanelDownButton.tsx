// Libraries
import React, {FC} from 'react'

// Components
import {SquareButton, IconFont, ComponentStatus} from '@influxdata/clockface'

interface Props {
  onMoveDown?: () => void
}

const MovePanelDownButton: FC<Props> = ({onMoveDown}) => {
  const status = onMoveDown ? ComponentStatus.Default : ComponentStatus.Disabled

  const handleClick = (): void => {
    if (onMoveDown) {
      onMoveDown()
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
