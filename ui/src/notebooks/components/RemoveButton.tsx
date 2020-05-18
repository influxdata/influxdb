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

  return <SquareButton icon={IconFont.Remove} onClick={onRemove} />
}

export default RemoveButton
