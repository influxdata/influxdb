// Libraries
import React, {FC} from 'react'
import {MAX_FILE_SIZE} from './DragAndDrop'

interface Props {
  uploadContent: string
  fileName: string
  fileSize: number
}

const DragAndDropHeader: FC<Props> = ({uploadContent, fileName, fileSize}) => {
  if (fileSize > MAX_FILE_SIZE) {
    return (
      <div
        className="drag-and-drop--header error"
        data-testid="dnd--header-error"
      >
        {fileName} is greater than 10MB
      </div>
    )
  }

  if (uploadContent) {
    return <div className="drag-and-drop--header selected">{fileName}</div>
  }

  return (
    <div className="drag-and-drop--header empty">
      Drop a file here or click to upload. Max size 10MB.
    </div>
  )
}

export default DragAndDropHeader
