import React, {SFC, ReactNode} from 'react'
import LoadingDots from 'src/shared/components/LoadingDots'
import classnames from 'classnames'
import {Status} from 'src/types'

interface Props {
  status: Status
  children?: ReactNode
}

const QueryStatus: SFC<Props> = ({status, children}) => {
  if (!status) {
    return <div className="query-editor--status">{children}</div>
  }

  if (status.loading) {
    return (
      <div className="query-editor--status">
        <LoadingDots className="query-editor--loading" />
        {children}
      </div>
    )
  }

  return (
    <div className="query-editor--status">
      <span
        className={classnames('query-status-output', {
          'query-status-output--error': status.error,
          'query-status-output--success': status.success,
          'query-status-output--warning': status.warn,
        })}
      >
        <span
          className={classnames('icon', {
            stop: status.error,
            checkmark: status.success,
            'alert-triangle': status.warn,
          })}
        />
        {status.error || status.warn || status.success}
      </span>
      {children}
    </div>
  )
}

export default QueryStatus
