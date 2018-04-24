import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import classnames from 'classnames'

import {dismissNotification as dismissNotificationAction} from 'shared/actions/notifications'

import {NOTIFICATION_TRANSITION} from 'shared/constants/index'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class Notification extends Component {
  constructor(props) {
    super(props)

    this.state = {
      opacity: 1,
      height: 0,
      dismissed: false,
    }
  }

  componentDidMount() {
    const {
      notification: {duration},
    } = this.props

    this.updateHeight()

    if (duration >= 0) {
      // Automatically dismiss notification after duration prop
      this.dismissTimer = setTimeout(this.handleDismiss, duration)
    }
  }

  updateHeight() {
    if (this.notificationRef) {
      const {height} = this.notificationRef.getBoundingClientRect()
      this.setState({height})
    }
  }

  componentWillUnmount() {
    clearTimeout(this.dismissTimer)
    clearTimeout(this.deleteTimer)
  }

  handleDismiss = () => {
    const {
      notification: {id},
      dismissNotification,
    } = this.props

    this.setState({dismissed: true})
    this.deleteTimer = setTimeout(
      () => dismissNotification(id),
      NOTIFICATION_TRANSITION
    )
  }

  onNotificationRef = ref => {
    this.notificationRef = ref
    this.updateHeight()
  }

  render() {
    const {
      notification: {type, message, icon},
    } = this.props
    const {height, dismissed} = this.state

    const notificationContainerClass = classnames('notification-container', {
      show: !!height,
      'notification-dismissed': dismissed,
    })
    const notificationClass = `notification notification-${type}`
    const notificationMargin = 4
    const style = {height: height + notificationMargin}

    return (
      <div className={notificationContainerClass} style={style}>
        <div className={notificationClass} ref={this.onNotificationRef}>
          <span className={`icon ${icon}`} />
          <div className="notification-message">{message}</div>
          <button className="notification-close" onClick={this.handleDismiss} />
        </div>
      </div>
    )
  }
}

const {func, number, shape, string} = PropTypes

Notification.propTypes = {
  notification: shape({
    id: string.isRequired,
    type: string.isRequired,
    message: string.isRequired,
    duration: number.isRequired,
    icon: string.isRequired,
  }).isRequired,
  dismissNotification: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  dismissNotification: bindActionCreators(dismissNotificationAction, dispatch),
})

export default connect(null, mapDispatchToProps)(Notification)
