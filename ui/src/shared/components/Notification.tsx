import React, {Component, CSSProperties} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {Notification as NotificationType} from 'src/types/notifications'

import classnames from 'classnames'

import {dismissNotification as dismissNotificationAction} from 'src/shared/actions/notifications'

import {NOTIFICATION_TRANSITION} from 'src/shared/constants/index'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  notification: NotificationType
  dismissNotification: (id: string) => void
}

interface State {
  opacity: number
  height: number
  dismissed: boolean
}

@ErrorHandling
class Notification extends Component<Props, State> {
  private notificationRef: HTMLElement
  private dismissalTimer: number
  private deletionTimer: number

  constructor(props) {
    super(props)

    this.state = {
      opacity: 1,
      height: 0,
      dismissed: false,
    }
  }

  public componentDidMount() {
    const {
      notification: {duration},
    } = this.props

    this.updateHeight()

    if (duration >= 0) {
      // Automatically dismiss notification after duration prop
      this.dismissalTimer = window.setTimeout(this.handleDismiss, duration)
    }
  }

  public componentWillUnmount() {
    clearTimeout(this.dismissalTimer)
    clearTimeout(this.deletionTimer)
  }

  public render() {
    const {
      notification: {message, icon},
    } = this.props

    return (
      <div className={this.containerClassname} style={this.notificationStyle}>
        <div
          className={this.notificationClassname}
          ref={this.handleNotificationRef}
        >
          <span className={`icon ${icon}`} />
          <div className="notification-message">{message}</div>
          <button className="notification-close" onClick={this.handleDismiss} />
        </div>
      </div>
    )
  }

  private get notificationClassname(): string {
    const {
      notification: {type},
    } = this.props

    return `notification notification-${type}`
  }

  private get containerClassname(): string {
    const {height, dismissed} = this.state

    return classnames('notification-container', {
      show: !!height,
      'notification-dismissed': dismissed,
    })
  }

  private get notificationStyle(): CSSProperties {
    const {height} = this.state

    const NOTIFICATION_MARGIN = 4

    return {height: height + NOTIFICATION_MARGIN}
  }

  private updateHeight = (): void => {
    if (this.notificationRef) {
      const {height} = this.notificationRef.getBoundingClientRect()
      this.setState({height})
    }
  }

  private handleDismiss = (): void => {
    const {
      notification: {id},
      dismissNotification,
    } = this.props

    this.setState({dismissed: true})
    this.deletionTimer = window.setTimeout(
      () => dismissNotification(id),
      NOTIFICATION_TRANSITION
    )
  }

  private handleNotificationRef = (ref: HTMLElement): void => {
    this.notificationRef = ref
    this.updateHeight()
  }
}

const mapDispatchToProps = dispatch => ({
  dismissNotification: bindActionCreators(dismissNotificationAction, dispatch),
})

export default connect(null, mapDispatchToProps)(Notification)
