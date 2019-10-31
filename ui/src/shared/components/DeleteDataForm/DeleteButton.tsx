// Libraries
import React, {FunctionComponent} from 'react'
import {
  Button,
  ComponentColor,
  ComponentStatus,
  Icon,
  IconFont,
} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  status: RemoteDataState
  valid: boolean
  onClick: () => any
}

const DeleteButton: FunctionComponent<Props> = ({status, valid, onClick}) => {
  if (status === RemoteDataState.Done) {
    return (
      <div className="delete-data-button delete-data-button--success">
        <Icon glyph={IconFont.Checkmark} />
        Success!
      </div>
    )
  }

  if (status === RemoteDataState.Error) {
    return (
      <div className="delete-data-button delete-data-button--error">
        <Icon glyph={IconFont.Remove} />
        An error occured and has been reported.
      </div>
    )
  }

  let deleteButtonStatus

  if (status === RemoteDataState.Loading) {
    deleteButtonStatus = ComponentStatus.Loading
  } else if (!valid) {
    deleteButtonStatus = ComponentStatus.Disabled
  } else {
    deleteButtonStatus = ComponentStatus.Default
  }

  return (
    <Button
      testID="confirm-delete-btn"
      text="Confirm Delete"
      color={ComponentColor.Danger}
      status={deleteButtonStatus}
      onClick={onClick}
    />
  )
}

export default DeleteButton
