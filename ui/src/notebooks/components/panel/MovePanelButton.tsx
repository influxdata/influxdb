// Libraries
import React, {FC, MouseEvent} from 'react'

// Components
import {SquareButton, IconFont, ComponentStatus} from '@influxdata/clockface'

interface Props {
  onClick?: () => void
  direction: 'up' | 'down'
}

const MovePanelUpButton: FC<Props> = ({onClick, direction}) => {
  const status = onClick ? ComponentStatus.Default : ComponentStatus.Disabled
  const icon = direction === 'up' ? IconFont.CaretUp : IconFont.CaretDown

  const handleClick = (e: MouseEvent<HTMLButtonElement>): void => {
    if (onClick) {
      e.stopPropagation()
      onClick()
    }
  }

  return <SquareButton icon={icon} onClick={handleClick} status={status} />
}

export default MovePanelUpButton
