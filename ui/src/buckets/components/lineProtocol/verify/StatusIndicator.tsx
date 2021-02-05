// Libraries
import React, {FC, useContext} from 'react'
import cn from 'classnames'

// Components
import {SparkleSpinner} from '@influxdata/clockface'
import {Context} from 'src/buckets/components/lineProtocol/LineProtocolWizard'

// Types
import {RemoteDataState} from 'src/types'

const getStatusText = (s, writeError) => {
  let status = ''
  let message = ''
  switch (s) {
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
      message = `${writeError}`
      break
  }

  return {
    status,
    message,
  }
}

const className = status =>
  cn(`line-protocol--status`, {
    loading: status === RemoteDataState.Loading,
    success: status === RemoteDataState.Done,
    error: status === RemoteDataState.Error,
  })

const StatusIndicator: FC = () => {
  const [{writeStatus, writeError}] = useContext(Context)

  return (
    <div className="line-protocol--spinner">
      <p data-testid="line-protocol--status" className={className(status)}>
        {getStatusText(writeStatus, writeError).status}
      </p>
      <SparkleSpinner loading={writeStatus} sizePixels={220} />
      <p className={className(status)}>
        {getStatusText(writeStatus, writeError).message}
      </p>
    </div>
  )
}

export default StatusIndicator
