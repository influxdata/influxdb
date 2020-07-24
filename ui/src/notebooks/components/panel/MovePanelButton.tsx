// Libraries
import React, {FC, MouseEvent} from 'react'

// Components
import {SquareButton, IconFont, ComponentStatus} from '@influxdata/clockface'

// Utils
import {event} from 'src/cloud/utils/reporting'

interface Props {
  onClick?: () => void
  direction: 'up' | 'down'
  active: boolean
}

const MovePanelUpButton: FC<Props> = ({onClick, direction, active}) => {
  const status = active ? ComponentStatus.Default : ComponentStatus.Disabled
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
      className="flow-move-cell-button"
    />
  )
}

export default MovePanelUpButton
