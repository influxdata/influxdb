// Libraries
import React, {FC, useContext} from 'react'
import classnames from 'classnames'

// Components
import {SparkleSpinner} from '@influxdata/clockface'
import {Context} from 'src/buckets/components/lineProtocol/LineProtocolWizard'

// Types
import {RemoteDataState} from 'src/types'

const StatusIndicator: FC = () => {
  const [{writeStatus: status, writeError}] = useContext(Context)
  const statusClassName = classnames(`line-protocol--status`, {
    loading: status === RemoteDataState.Loading,
    success: status === RemoteDataState.Done,
    error: status === RemoteDataState.Error,
  })

  const getStatusText = () => {
    let status = ''
    let message = ''
    switch (status) {
      case RemoteDataState.Loading:
        status = 'Loading...'
        message = 'Just a moment'
        break
      case RemoteDataState.Done:
        status = 'Data Written Successfully'
        message = 'Hooray!'
        break
      case RemoteDataState.Error:
        status = 'Unable to Write Data'
        message = `Error: ${writeError}`
        break
    }

    return {
      status,
      message,
    }
  }

  const statusText = getStatusText()

  return (
    <div className="line-protocol--spinner">
      <p data-testid="line-protocol--status" className={statusClassName}>
        {statusText.status}
      </p>
      <SparkleSpinner loading={status} sizePixels={220} />
      <p className={statusClassName}>{statusText.message}</p>
    </div>
  )
}

export default StatusIndicator
