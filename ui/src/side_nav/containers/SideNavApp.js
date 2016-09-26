import React, {PropTypes} from 'react';
import SideNav from '../components/SideNav';

const {func, string} = PropTypes;
const SideNavApp = React.createClass({
  propTypes: {
    currentLocation: string.isRequired,
    addFlashMessage: func.isRequired,
    sourceID: string.isRequired,
  },

  contextTypes: {
    canViewChronograf: PropTypes.bool,
  },

  getInitialState() {
    return {
      clusters: [],
      clusterToUpdate: '',
    };
  },

  render() {
    const {currentLocation, sourceID} = this.props;
    const {canViewChronograf} = this.context;

    return (
      <SideNav
        sourceID={sourceID}
        isAdmin={true}
        canViewChronograf={canViewChronograf}
        location={currentLocation}
      />
    );
  },

});

export default SideNavApp;
