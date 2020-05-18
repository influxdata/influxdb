// Libraries
import React, {FC} from 'react'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'

interface Props {
  onRemove?: () => void
}

const RemoveButton: FC<Props> = ({onRemove}) => {
  if (!onRemove) {
    return null
  }

  const handleClick = (): void => {
    onRemove()
  }

  return <SquareButton icon={IconFont.Remove} onClick={handleClick} />
}

export default RemoveButton
