import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {dismissNotification as dismissNotificationAction} from 'shared/actions/notifications'

class Notification extends Component {
  constructor(props) {
    super(props)
  }

  getOpacity = () => {
    const {notification: {created, duration}} = this.props
    const expirationTime = created + duration

    if (duration === -1) {
      // Notification is present until user dismisses it
      return false
    }

    if (expirationTime < Date.now()) {
      return true
      // dismissNotification(id)
    }
  }

  render() {
    const {
      notification: {id, type, message, icon},
      dismissNotification,
    } = this.props

    const notificationClass = `alert alert-${type}`

    const notificationStyle = {
      opacity: this.getOpacity(),
    }

    return (
      <div className={notificationClass}>
        {icon && <span className={`icon ${icon}`} />}
        <div className="alert-message">
          {message}
        </div>
        <button className="alert-close" onClick={dismissNotification(id)}>
          <span className="icon remove" />
        </button>
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
    created: number.isRequired,
    duration: number.isRequired,
    icon: string,
  }),
  dismissNotification: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  dismissNotification: bindActionCreators(dismissNotificationAction, dispatch),
})

export default connect(null, mapDispatchToProps)(Notification)
