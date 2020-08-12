// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Context} from './LineProtocolWizard'
import {
  Button,
  ButtonType,
  ComponentColor,
  ComponentSize,
} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  onClose: () => void
  onCancel: () => void
}

const UploadFileButtons: FC<Props> = ({onClose, onCancel}) => {
  const [{writeStatus}] = useContext(Context)

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

  return null
}

export default UploadFileButtons
