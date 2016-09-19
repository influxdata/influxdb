import React, {PropTypes} from 'react';
import SideNav from '../components/SideNav';

const {func, string} = PropTypes;
const SideNavApp = React.createClass({
  propTypes: {
    currentLocation: string.isRequired,
    addFlashMessage: func.isRequired,
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
    const {currentLocation} = this.props;
    const {canViewChronograf} = this.context;

    return (
      <SideNav
        isAdmin={true}
        canViewChronograf={canViewChronograf}
        location={currentLocation}
      />
    );
  },

});

export default SideNavApp;
