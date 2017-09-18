import React, {PropTypes} from 'react'
import LoadingDots from 'shared/components/LoadingDots'
import classnames from 'classnames'

const QueryStatus = ({status, children}) => {
  if (!status) {
    return (
      <div className="query-editor--status">
        {children}
      </div>
    )
  }

  if (status.loading) {
    return (
      <div className="query-editor--status">
        <LoadingDots />
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

const {node, shape, string} = PropTypes

QueryStatus.propTypes = {
  status: shape({
    error: string,
    success: string,
    warn: string,
  }),
  children: node,
}

export default QueryStatus
