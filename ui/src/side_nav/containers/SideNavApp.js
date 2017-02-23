import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import SideNav from '../components/SideNav';

const {
  func,
  string,
  shape,
  bool,
} = PropTypes

const SideNavApp = React.createClass({
  propTypes: {
    currentLocation: string.isRequired,
    addFlashMessage: func.isRequired,
    sourceID: string.isRequired,
    me: shape({
      email: string,
    }),
    inPresentationMode: bool.isRequired,
  },

  render() {
    const {me, currentLocation, sourceID, inPresentationMode} = this.props;

    return (
      <SideNav
        sourceID={sourceID}
        location={currentLocation}
        me={me}
        isHidden={inPresentationMode}
      />
    );
  },
});

function mapStateToProps(state) {
  return {
    me: state.me,
    inPresentationMode: state.appUI.presentationMode,
  };
}

export default connect(mapStateToProps)(SideNavApp);
