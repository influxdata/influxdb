import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import classnames from 'classnames'

import {dismissNotification as dismissNotificationAction} from 'shared/actions/notifications'

class Notification extends Component {
  constructor(props) {
    super(props)

    this.state = {
      opacity: 1,
      height: 0,
      dismiss: false,
    }
  }

  componentDidMount = () => {
    const {notification: {duration}} = this.props

    // Trigger animation in
    const {height} = this.notificationRef.getBoundingClientRect()
    this.setState({height})

    if (duration >= 0) {
      // Automatically dismiss notification after duration prop
      this.dismissTimer = setTimeout(this.handleDismiss, duration)
    }
  }

  componentWillUnmount = () => {
    clearTimeout(this.dismissTimer)
    clearTimeout(this.deleteTimer)
  }

  handleDismiss = () => {
    const {notification: {id}, dismissNotification} = this.props

    this.setState({dismiss: true})
    this.deleteTimer = setTimeout(() => dismissNotification(id), 250)
  }

  render() {
    const {notification: {type, message, icon}} = this.props
    const {height, dismiss} = this.state

    const notificationContainerClass = classnames('notification-container', {
      show: !!height,
      'notification-dismissed': dismiss,
    })
    const notificationClass = `notification notification-${type}`
    const notificationMargin = 4

    return (
      <div
        className={notificationContainerClass}
        style={{height: height + notificationMargin}}
      >
        <div
          className={notificationClass}
          ref={r => (this.notificationRef = r)}
        >
          <span className={`icon ${icon}`} />
          <div className="notification-message">
            {message}
          </div>
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
  }),
  dismissNotification: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  dismissNotification: bindActionCreators(dismissNotificationAction, dispatch),
})

export default connect(null, mapDispatchToProps)(Notification)
