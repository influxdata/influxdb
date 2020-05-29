// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Icon, IconFont} from '@influxdata/clockface'

interface Props {
  testID?: string
  label: string
  icon: IconFont
  onClick: () => void
  onHide?: () => void
}

const CellContextItem: FunctionComponent<Props> = ({
  icon,
  label,
  testID,
  onHide,
  onClick,
}) => {
  const handleClick = (): void => {
    onHide && onHide()
    onClick()
  }

  return (
    <div
      className="cell--context-item"
      onClick={handleClick}
      data-testid={testID}
    >
      <Icon glyph={icon} />
      {label}
    </div>
  )
}

export default CellContextItem
