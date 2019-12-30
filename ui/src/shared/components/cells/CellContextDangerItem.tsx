// Libraries
import React, {FunctionComponent, useState} from 'react'

// Components
import {
  Icon,
  IconFont,
  Button,
  ComponentColor,
  ComponentSize,
} from '@influxdata/clockface'
import {ButtonShape} from 'src/clockface'

interface Props {
  testID?: string
  label: string
  icon?: IconFont
  onClick: () => void
  onHide?: () => void
  confirmationText?: string
}

const CellContextItem: FunctionComponent<Props> = ({
  icon = IconFont.Trash,
  label,
  testID,
  onHide,
  onClick,
  confirmationText = 'Confirm Delete',
}) => {
  const [confirming, setConfirmationState] = useState<boolean>(false)

  const toggleConfirmationState = (): void => {
    setConfirmationState(true)
  }

  const handleClick = (): void => {
    onHide && onHide()
    onClick()
  }

  if (confirming) {
    return (
      <div className="cell--context-item cell--context-item__confirm">
        <Button
          color={ComponentColor.Danger}
          text={confirmationText}
          onClick={handleClick}
          size={ComponentSize.ExtraSmall}
          shape={ButtonShape.StretchToFit}
          testID={`${testID}-confirm`}
        />
      </div>
    )
  }

  return (
    <div
      className="cell--context-item cell--context-item__danger"
      onClick={toggleConfirmationState}
      data-testid={testID}
    >
      <Icon glyph={icon} />
      {label}
    </div>
  )
}

export default CellContextItem
