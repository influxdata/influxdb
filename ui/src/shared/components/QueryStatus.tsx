import React, {Component, ReactElement} from 'react'
import LoadingDots from 'src/shared/components/LoadingDots'
import classnames from 'classnames'

interface Status {
  error: string
  success: string
  warn: string
  loading: boolean
}

interface Props {
  children: ReactElement<any>
  status: Status
}

class QueryStatus extends Component<Props> {
  public render() {
    const {status, children} = this.props

    if (!status) {
      return <div className="query-editor--status">{children}</div>
    }

    if (!!status.loading) {
      return (
        <div className="query-editor--status">
          <LoadingDots />
          {children}
        </div>
      )
    }

    return (
      <div className="query-editor--status">
        <span className={this.className}>
          {this.icon}
          {status.error || status.warn || status.success}
        </span>
        {children}
      </div>
    )
  }

  private get className(): string {
    const {status} = this.props

    return classnames('query-status-output', {
      'query-status-output--error': status.error,
      'query-status-output--success': status.success,
      'query-status-output--warning': status.warn,
    })
  }

  private get icon(): JSX.Element {
    const {status} = this.props
    return (
      <span
        className={classnames('icon', {
          stop: status.error,
          checkmark: status.success,
          'alert-triangle': status.warn,
        })}
      />
    )
  }
}

export default QueryStatus
