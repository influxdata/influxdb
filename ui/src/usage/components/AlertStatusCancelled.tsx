import React, {FC} from 'react'

import {Alert, ComponentColor, IconFont} from '@influxdata/clockface'

interface Props {
  className?: string
  isOperator: boolean
}

const AlertStatusCancelled: FC<Props> = ({className, isOperator}) => {
  const messageText = isOperator
    ? 'This account has been cancelled'
    : 'Hey there, it looks like your account has been cancelled. Please contact support at support@influxdata.com to extract data from your buckets.'

  return (
    <Alert
      color={ComponentColor.Danger}
      icon={IconFont.AlertTriangle}
      className={className}
    >
      {messageText}
    </Alert>
  )
}

export default AlertStatusCancelled
