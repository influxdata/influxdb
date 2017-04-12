import React, {PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import SideNavContainer from 'src/side_nav'
import Notifications from 'shared/components/Notifications'

import {publishNotification} from 'src/shared/actions/notifications'

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

  handleAddFlashMessage({type, text}) {
    const {notify} = this.props

    notify(type, text)
  },

  render() {
    const {params: {sourceID}, location} = this.props

    return (
      <div className="chronograf-root">
        <SideNavContainer
          sourceID={sourceID}
          addFlashMessage={this.handleAddFlashMessage}
          currentLocation={this.props.location.pathname}
        />
        <Notifications location={location} />
        {this.props.children && React.cloneElement(this.props.children, {
          addFlashMessage: this.handleAddFlashMessage,
        })}
      </div>
    )
  },
})

const mapDispatchToProps = (dispatch) => ({
  notify: bindActionCreators(publishNotification, dispatch),
})

export default connect(null, mapDispatchToProps)(App)
