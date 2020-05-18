// Libraries
import React, {FC} from 'react'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'

interface Props {
  id: string
  onRemove?: (id: string) => void
}

const RemoveButton: FC<Props> = ({id, onRemove}) => {
  if (!onRemove) {
    return null
  }

  const handleClick = (): void => {
    onRemove(id)
  }

  return <SquareButton icon={IconFont.Remove} onClick={handleClick} />
}

export default RemoveButton
