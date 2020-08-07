// Libraries
import React, {FC} from 'react'
import {WaitingText} from '@influxdata/clockface'

import {RemoteDataState} from 'src/types'

const MAX_FILE_SIZE = 1e7

interface Props {
  uploadContent: string
  fileName: string
  fileSize: number
  readStatus: RemoteDataState
}

const DragAndDropHeader: FC<Props> = ({
  uploadContent,
  fileName,
  fileSize,
  readStatus,
}) => {
  if (readStatus === RemoteDataState.Loading) {
    return <WaitingText text="Reading file" testID="waiting-text" />
  }

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
