import React, {PropTypes} from 'react'
import {connect} from 'react-redux'
import classnames from 'classnames'
import SideNavContainer from 'src/side_nav'
import {
  publishNotification as publishNotificationAction,
  dismissNotification as dismissNotificationAction,
  dismissAllNotifications as dismissAllNotificationsAction,
} from 'src/shared/actions/notifications'

const {
  node,
  shape,
  string,
  func,
} = PropTypes

const App = React.createClass({
  propTypes: {
    children: node.isRequired,
    location: shape({
      pathname: string,
    }),
    params: shape({
      sourceID: string.isRequired,
    }).isRequired,
    publishNotification: func.isRequired,
    dismissNotification: func.isRequired,
    dismissAllNotifications: func.isRequired,
    notifications: shape({
      success: string,
      error: string,
      warning: string,
    }),
  },

  handleNotification({type, text}) {
    const validTypes = ['error', 'success', 'warning']
    if (!validTypes.includes(type) || text === undefined) {
      console.error("handleNotification must have a valid type and text") // eslint-disable-line no-console
    }
    this.props.publishNotification(type, text)
  },

  handleDismissNotification(type) {
    this.props.dismissNotification(type)
  },

  componentWillReceiveProps(nextProps) {
    if (nextProps.location.pathname !== this.props.location.pathname) {
      this.props.dismissAllNotifications()
    }
  },

  render() {
    const {params: {sourceID}} = this.props

    return (
      <div className="chronograf-root">
        <SideNavContainer
          sourceID={sourceID}
          addFlashMessage={this.handleNotification}
          currentLocation={this.props.location.pathname}
        />
        {this.renderNotifications()}
        {this.props.children && React.cloneElement(this.props.children, {
          addFlashMessage: this.handleNotification,
        })}
      </div>
    )
  },

  renderNotifications() {
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
  },

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
        {message}{this.renderDismiss(type)}
      </div>
    )
  },

  renderDismiss(type) {
    return (
      <button className="close" data-dismiss="alert" aria-label="Close" onClick={() => this.handleDismissNotification(type)}>
        <span className="icon remove"></span>
      </button>
    )
  },
})

function mapStateToProps(state) {
  return {
    notifications: state.notifications,
  }
}

export default connect(mapStateToProps, {
  publishNotification: publishNotificationAction,
  dismissNotification: dismissNotificationAction,
  dismissAllNotifications: dismissAllNotificationsAction,
})(App)
