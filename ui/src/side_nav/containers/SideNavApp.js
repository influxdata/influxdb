import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import SideNav from '../components/SideNav';

const {func, string, shape} = PropTypes;
const SideNavApp = React.createClass({
  propTypes: {
    currentLocation: string.isRequired,
    addFlashMessage: func.isRequired,
    sourceID: string.isRequired,
    me: shape({
      email: string,
    }),
  },

  render() {
    const {me, currentLocation, sourceID} = this.props;

    return (
      <SideNav
        sourceID={sourceID}
        location={currentLocation}
        me={me}
      />
    );
  },

});

function mapStateToProps(state) {
  return {
    me: state.me,
  };
}

export default connect(mapStateToProps)(SideNavApp);
