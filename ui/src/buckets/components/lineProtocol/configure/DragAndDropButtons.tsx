import React, {FC} from 'react'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ButtonType,
} from '@influxdata/clockface'

interface Props {
  fileSize: number
  uploadContent: string
  onCancel: () => void
  onSubmit: () => void
}

const MAX_FILE_SIZE = 1e7 // 10MB

const DragAndDropButtons: FC<Props> = ({
  fileSize,
  uploadContent,
  onCancel,
  onSubmit,
}) => {
  if (fileSize > MAX_FILE_SIZE) {
    return (
      <span className="drag-and-drop--buttons">
        <Button
          color={ComponentColor.Default}
          text="Cancel"
          size={ComponentSize.Medium}
          type={ButtonType.Button}
          onClick={onCancel}
          testID="cancel-upload--button"
        />
      </span>
    )
  }

  if (!uploadContent) {
    return null
  }

  return (
    <span className="drag-and-drop--buttons">
      <Button
        color={ComponentColor.Primary}
        text="Write Data"
        size={ComponentSize.Medium}
        type={ButtonType.Submit}
        onClick={onSubmit}
        testID="write-data--button"
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
