// Libraries
import React, {FC, useContext} from 'react'
import cn from 'classnames'

// Components
import {Context} from 'src/buckets/components/lineProtocol/LineProtocolWizard'
import StatusIndicator from '../verify/StatusIndicator'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  uploadContent: string
}

const DragInfo: FC<Props> = ({uploadContent}) => {
  const [{writeStatus}] = useContext(Context)
  const className = cn('drag-and-drop--graphic', {success: uploadContent})

  if (writeStatus !== RemoteDataState.NotStarted) {
    return <StatusIndicator />
  }

  return <div className={className} />
}

export default DragInfo
