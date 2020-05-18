// Libraries
import React, {FC} from 'react'

// Components
import {SquareButton, IconFont, ComponentStatus} from '@influxdata/clockface'

interface Props {
  onClick?: () => void
  direction: 'up' | 'down'
}

const MovePanelUpButton: FC<Props> = ({onClick, direction}) => {
  const status = onClick ? ComponentStatus.Default : ComponentStatus.Disabled
  const icon = direction === 'up' ? IconFont.CaretUp : IconFont.CaretDown

  const handleClick = (): void => {
    if (onClick) {
      onClick()
    }
  }

  return <SquareButton icon={icon} onClick={handleClick} status={status} />
}

export default MovePanelUpButton
