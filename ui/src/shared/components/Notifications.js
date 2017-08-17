import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {
  publishNotification as publishNotificationAction,
  dismissNotification as dismissNotificationAction,
  dismissAllNotifications as dismissAllNotificationsAction,
} from 'shared/actions/notifications'

class Notifications extends Component {
  constructor(props) {
    super(props)

    this.renderNotification = ::this.renderNotification
    this.renderDismiss = ::this.renderDismiss
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.location.pathname !== this.props.location.pathname) {
      this.props.dismissAllNotifications()
    }
  }

  renderNotification(type, message) {
    if (!message) {
      return null
    }
    const cls = classnames('alert', {
      'alert-danger': type === 'error',
      'alert-success': type === 'success',
      'alert-warning': type === 'warning',
    })
    return (
      <div className={cls} role="alert">
        {message}
        {this.renderDismiss(type)}
      </div>
    )
  }

  handleDismiss = type => () => this.props.dismissNotification(type)

  renderDismiss(type) {
    return (
      <button
        className="close"
        data-dismiss="alert"
        aria-label="Close"
        onClick={this.handleDismiss(type)}
      >
        <span className="icon remove" />
      </button>
    )
  }

  render() {
    const {success, error, warning} = this.props.notifications
    if (!success && !error && !warning) {
      return null
    }

    return (
      <div className="flash-messages">
        {this.renderNotification('success', success)}
        {this.renderNotification('error', error)}
        {this.renderNotification('warning', warning)}
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

Notifications.propTypes = {
  location: shape({
    pathname: string.isRequired,
  }).isRequired,
  publishNotification: func.isRequired,
  dismissNotification: func.isRequired,
  dismissAllNotifications: func.isRequired,
  notifications: shape({
    success: string,
    error: string,
    warning: string,
  }),
}

const mapStateToProps = ({notifications}) => ({
  notifications,
})

const mapDispatchToProps = dispatch => ({
  publishNotification: bindActionCreators(publishNotificationAction, dispatch),
  dismissNotification: bindActionCreators(dismissNotificationAction, dispatch),
  dismissAllNotifications: bindActionCreators(
    dismissAllNotificationsAction,
    dispatch
  ),
})

export default connect(mapStateToProps, mapDispatchToProps)(
  withRouter(Notifications)
)
