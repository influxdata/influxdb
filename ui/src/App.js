import React, {PropTypes} from 'react'
import {connect} from 'react-redux'
import SideNavContainer from 'src/side_nav'
import Notifications from 'shared/components/Notifications'
import {
  publishNotification as publishNotificationAction,
} from 'src/shared/actions/notifications'

const {
  func,
  node,
  shape,
  string,
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
    notify: func.isRequired,
  },

  render() {
    const {params: {sourceID}, location, notify} = this.props

    return (
      <div className="chronograf-root">
        <SideNavContainer
          sourceID={sourceID}
          addFlashMessage={notify}
          currentLocation={this.props.location.pathname}
        />
        <Notifications location={location} />
        {this.props.children && React.cloneElement(this.props.children, {
          addFlashMessage: notify,
        })}
      </div>
    )
  },
})

export default connect(null, {
  notify: publishNotificationAction,
})(App)
