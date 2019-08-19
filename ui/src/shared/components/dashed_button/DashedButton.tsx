// Libraries
import React, {FC, MouseEvent} from 'react'
import classnames from 'classnames'

// Types
import {ComponentColor, ComponentSize} from '@influxdata/clockface'

interface Props {
  text: string
  onClick: (e: MouseEvent) => void
  color: ComponentColor
  size: ComponentSize
}

const DashedButton: FC<Props> = ({
  text,
  onClick,
  color = ComponentColor.Primary,
  size = ComponentSize.Medium,
}) => {
  const classname = classnames('dashed-button', {
    [`dashed-button__${color}`]: color,
    [`dashed-button__${size}`]: size,
  })

  return (
    <button className={classname} onClick={onClick} type="button">
      {text}
    </button>
  )
}

export default DashedButton
