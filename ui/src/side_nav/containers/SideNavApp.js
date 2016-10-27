import React, {PropTypes} from 'react';
import SideNav from '../components/SideNav';

const {func, string} = PropTypes;
const SideNavApp = React.createClass({
  propTypes: {
    currentLocation: string.isRequired,
    addFlashMessage: func.isRequired,
    sourceID: string.isRequired,
    explorationID: string,
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
    const {currentLocation, sourceID, explorationID} = this.props;
    const {canViewChronograf} = this.context;

    return (
      <SideNav
        sourceID={sourceID}
        isAdmin={true}
        canViewChronograf={canViewChronograf}
        location={currentLocation}
        explorationID={explorationID}
      />
    );
  },

});

export default SideNavApp;
