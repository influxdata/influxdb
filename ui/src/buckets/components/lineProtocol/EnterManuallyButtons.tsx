// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Context} from './LineProtocolWizard'
import {
  Button,
  ButtonType,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  onSubmit: () => void
  onCancel: () => void
  onClose: () => void
}

const EnterManuallyButtons: FC<Props> = ({onSubmit, onCancel, onClose}) => {
  const [{writeStatus, body}] = useContext(Context)
  const status = body ? ComponentStatus.Default : ComponentStatus.Disabled

  if (writeStatus === RemoteDataState.Error) {
    return (
      <Button
        color={ComponentColor.Default}
        text="Cancel"
        size={ComponentSize.Medium}
        type={ButtonType.Button}
        onClick={onCancel}
        testID="lp-cancel--button"
      />
    )
  }

  if (writeStatus === RemoteDataState.Done) {
    return (
      <Button
        color={ComponentColor.Default}
        text="Close"
        size={ComponentSize.Medium}
        type={ButtonType.Button}
        onClick={onClose}
        testID="lp-close--button"
      />
    )
  }

  return (
    <Button
      color={ComponentColor.Primary}
      text="Write Data"
      size={ComponentSize.Medium}
      type={ButtonType.Button}
      status={status}
      testID="lp-write-data--button"
      onClick={onSubmit}
    />
  )
}

export default EnterManuallyButtons
