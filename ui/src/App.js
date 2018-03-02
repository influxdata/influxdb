import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import SideNav from 'src/side_nav'
import Notifications from 'shared/components/Notifications'

import {publishNotification as publishNotificationAction} from 'shared/actions/notifications'

class App extends Component {
  notify = notification => {
    const {publishNotification} = this.props

    publishNotification(notification)
  }

  render() {
    return (
      <div className="chronograf-root">
        <Notifications />
        <SideNav />
        {this.props.children &&
          React.cloneElement(this.props.children, {
            addFlashMessage: this.notify,
          })}
      </div>
    )
  }
}

const {func, node} = PropTypes

App.propTypes = {
  children: node.isRequired,
  publishNotification: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  publishNotification: bindActionCreators(publishNotificationAction, dispatch),
})

export default connect(null, mapDispatchToProps)(App)
