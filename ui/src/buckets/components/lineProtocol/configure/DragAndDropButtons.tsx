import React, {FC} from 'react'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ButtonType,
} from '@influxdata/clockface'

interface Props {
  uploadContent: string
  onCancel: () => void
}

const DragAndDropButtons: FC<Props> = ({uploadContent, onCancel}) => {
  if (!uploadContent) {
    return null
  }

  return (
    <span className="drag-and-drop--buttons">
      <Button
        color={ComponentColor.Default}
        text="Cancel"
        size={ComponentSize.Medium}
        type={ButtonType.Button}
        onClick={onCancel}
      />
    </span>
  )

  return (
    <span className="drag-and-drop--buttons">
      <Button
        color={ComponentColor.Primary}
        text="Write Data"
        size={ComponentSize.Medium}
        type={ButtonType.Submit}
        onClick={this.handleSubmit}
      />
      <Button
        color={ComponentColor.Default}
        text="Cancel"
        size={ComponentSize.Medium}
        type={ButtonType.Submit}
        onClick={onCancel}
        testID="cancel-upload--button"
      />
    </span>
  )
}

export default DragAndDropButtons
