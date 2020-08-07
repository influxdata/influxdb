// Libraries
import React, {FC, useContext} from 'react'
import {useHistory} from 'react-router'

// Actions
import {reset} from 'src/buckets/components/lineProtocol/LineProtocol.creators'

// Components
import {Context} from 'src/buckets/components/lineProtocol/LineProtocolWizard'
import EnterManuallyButtons from './EnterManuallyButtons'
import UploadFileButtons from './UploadFileButtons'

interface Props {
  onSubmit: () => void
}

const LineProtocolButtons: FC<Props> = ({onSubmit}) => {
  const history = useHistory()
  const [{tab}, dispatch] = useContext(Context)
  const handleCancel = () => {
    dispatch(reset())
  }

  const handleClose = () => {
    history.goBack()
  }

  if (tab === 'Enter Manually') {
    return (
      <EnterManuallyButtons
        onClose={handleClose}
        onCancel={handleCancel}
        onSubmit={onSubmit}
      />
    )
  }

  if (tab === 'Upload File') {
    return <UploadFileButtons onClose={handleClose} onCancel={handleCancel} />
  }

  return null
}

export default LineProtocolButtons
