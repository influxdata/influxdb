import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'

import SideNav from 'src/side_nav/components/SideNav'

const {
  bool,
  shape,
  string,
} = PropTypes

const SideNavApp = React.createClass({
  propTypes: {
    params: shape({
      sourceID: string.isRequired,
    }).isRequired,
    location: shape({
      pathname: string.isRequired,
    }).isRequired,
    me: shape({
      email: string,
    }),
    inPresentationMode: bool.isRequired,
  },

  render() {
    const {me, params: {sourceID}, location: {pathname: location}, inPresentationMode} = this.props

    return (
      <SideNav
        sourceID={sourceID}
        location={location}
        me={me}
        isHidden={inPresentationMode}
      />
    )
  },
})

const mapStateToProps = ({me, app: {ephemeral: {inPresentationMode}}}) => ({
  me,
  inPresentationMode,
})

export default connect(mapStateToProps)(withRouter(SideNavApp))
