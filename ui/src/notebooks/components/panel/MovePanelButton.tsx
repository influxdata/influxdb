// Libraries
import React, {FC, MouseEvent} from 'react'

// Components
import {SquareButton, IconFont, ComponentStatus} from '@influxdata/clockface'

// Utils
import {event} from 'src/notebooks/shared/event'

interface Props {
  onClick?: () => void
  direction: 'up' | 'down'
}

const MovePanelUpButton: FC<Props> = ({onClick, direction}) => {
  const status = onClick ? ComponentStatus.Default : ComponentStatus.Disabled
  const icon = direction === 'up' ? IconFont.CaretUp : IconFont.CaretDown

  const handleClick = (e: MouseEvent<HTMLButtonElement>): void => {
    if (onClick) {
      event('Notebook Panel Moved', {
        direction,
      })

      e.stopPropagation()
      onClick()
    }
  }

  const title = `Move this cell ${direction}`

  return (
    <SquareButton
      icon={icon}
      onClick={handleClick}
      titleText={title}
      status={status}
    />
  )
}

export default MovePanelUpButton
