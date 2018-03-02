import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import classnames from 'classnames'

import {
  dismissNotification as dismissNotificationAction,
  deleteNotification as deleteNotificationAction,
} from 'shared/actions/notifications'

class Notification extends Component {
  constructor(props) {
    super(props)

    this.state = {
      opacity: 1,
      height: 0,
    }
  }

  componentDidMount = () => {
    const {notification: {duration}} = this.props

    // Trigger animation in
    const {height} = this.notificationRef.getBoundingClientRect()
    this.setState({height})

    if (duration >= 0) {
      // Automatically dismiss notification after duration prop
      window.setTimeout(this.handleDismiss, duration)
    }
  }

  componentDidUpdate = () => {
    const {notification: {dismiss}} = this.props

    if (dismiss) {
      window.setTimeout(this.handleDelete, 800)
    }
  }

  handleDelete = () => {
    const {notification: {id}, deleteNotification} = this.props
    deleteNotification(id)
  }

  handleDismiss = () => {
    const {notification: {id, dismiss}, dismissNotification} = this.props

    return dismiss ? null : dismissNotification(id)
  }

  render() {
    const {notification: {type, message, icon, dismiss}} = this.props
    const {height} = this.state

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
          <button className="notification-close" onClick={this.handleDismiss}>
            <span className="icon remove" />
          </button>
        </div>
      </div>
    )
  }
}

const {func, bool, number, shape, string} = PropTypes

Notification.defaultProps = {
  notification: {
    icon: 'zap',
  },
}

Notification.propTypes = {
  notification: shape({
    id: string.isRequired,
    type: string.isRequired,
    message: string.isRequired,
    created: number.isRequired,
    duration: number.isRequired,
    icon: string,
    dismiss: bool,
  }),
  dismissNotification: func.isRequired,
  deleteNotification: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  dismissNotification: bindActionCreators(dismissNotificationAction, dispatch),
  deleteNotification: bindActionCreators(deleteNotificationAction, dispatch),
})

export default connect(null, mapDispatchToProps)(Notification)
