import React, {PropTypes} from 'react';
import {getClusters, updateCluster} from 'shared/apis';
import SideNav from '../components/SideNav';
import {webUserShape} from 'src/utils/propTypes';

const {func, string} = PropTypes;
const SideNavApp = React.createClass({
  propTypes: {
    clusterID: string.isRequired,
    currentLocation: string.isRequired,
    addFlashMessage: func.isRequired,
  },

  contextTypes: {
    me: webUserShape,
    canViewChronograf: PropTypes.bool,
  },

  getInitialState() {
    return {
      clusters: [],
      clusterToUpdate: '',
    };
  },

  getPageData() {
    getClusters().then(({data: clusters}) => {
      this.setState({clusters});
    });
  },

  componentDidMount() {
    this.getPageData();
  },

  handleRenameCluster(displayName) {
    const {addFlashMessage} = this.props;
    updateCluster(this.state.clusterToUpdate, displayName).then(() => {
      this.getPageData();
      addFlashMessage({
        type: 'success',
        text: 'Cluster name changed successully',
      });
    }).catch(() => {
      addFlashMessage({
        type: 'error',
        text: 'Something went wrong while renaming the cluster',
      });
    });
  },

  handleOpenRenameModal(clusterToUpdate) {
    this.setState({clusterToUpdate});
  },

  render() {
    const {clusterID, currentLocation} = this.props;
    const {me, canViewChronograf} = this.context;

    return (
      <SideNav
        isAdmin={!!me && me.admin}
        canViewChronograf={canViewChronograf}
        location={currentLocation}
        clusterID={clusterID}
        onRenameCluster={this.handleRenameCluster}
        onOpenRenameModal={this.handleOpenRenameModal}
        clusters={this.state.clusters}
      />
    );
  },

});

export default SideNavApp;
