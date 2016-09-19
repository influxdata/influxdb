import React, {PropTypes} from 'react';
import CreateClusterAdmin from './components/CreateClusterAdmin';
import CreateWebAdmin from './components/CreateWebAdmin';
import NameCluster from './components/NameCluster';
import NoCluster from './components/NoCluster';
import {withRouter} from 'react-router';
import {
  createWebAdmin,
  getClusters,
  createClusterUserAtSetup,
  updateClusterAtSetup,
} from 'shared/apis';

const SignUpApp = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      step: PropTypes.string.isRequired,
    }).isRequired,
    router: PropTypes.shape({
      push: PropTypes.func.isRequired,
      replace: PropTypes.func.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      clusterDisplayName: null,
      clusterIDs: null,
      activeClusterID: null,
      clusterUser: '',
    };
  },

  componentDidMount() {
    getClusters().then(({data: clusters}) => {
      const clusterIDs = clusters.map((c) => c.cluster_id); // TODO: handle when the first cluster is down...
      this.setState({
        clusterIDs,
        activeClusterID: clusterIDs[0],
      });
    });
  },

  handleNameCluster(clusterDisplayName) {
    this.setState({clusterDisplayName}, () => {
      this.props.router.replace('/signup/admin/2');
    });
  },

  handleCreateClusterAdmin(username, password) {
    const {activeClusterID, clusterDisplayName} = this.state;
    createClusterUserAtSetup(activeClusterID, username, password).then(() => {
      updateClusterAtSetup(activeClusterID, clusterDisplayName).then(() => {
        this.setState({clusterUser: username}, () => {
          this.props.router.replace('/signup/admin/3');
        });
      });
    });
  },

  handleCreateWebAdmin(firstName, lastName, email, password, confirmation, clusterLinks) {
    createWebAdmin({firstName, lastName, email, password, confirmation, clusterLinks}).then(() => {
      window.location.replace('/');
    });
  },

  render() {
    const {params: {step}, router} = this.props;
    const {clusterDisplayName, clusterIDs} = this.state;

    if (!['1', '2', '3'].includes(step)) {
      router.replace('/signup/admin/1');
    }

    if (clusterIDs === null) {
      return null; // spinner?
    }

    if (!clusterIDs.length) {
      return <NoCluster />;
    }

    if (step === '1' || !clusterDisplayName) {
      return <NameCluster onNameCluster={this.handleNameCluster} />;
    }

    if (step === '2') {
      return <CreateClusterAdmin onCreateClusterAdmin={this.handleCreateClusterAdmin} />;
    }

    if (step === '3') {
      return <CreateWebAdmin onCreateWebAdmin={this.handleCreateWebAdmin} />;
    }
  },
});

export default withRouter(SignUpApp);
