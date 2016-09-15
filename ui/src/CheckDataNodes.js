import React, {PropTypes} from 'react';
import NoClusterError from 'shared/components/NoClusterError';
import NoClusterLinksError from 'shared/components/NoClusterLinksError';
import {webUserShape} from 'src/utils/propTypes';
import {showCluster} from 'src/shared/apis';

// Acts as a 'router middleware'. The main `App` component is responsible for
// getting the list of data nodes, but not every page requires them to function.
// Routes that do require data nodes can be nested under this component.
const CheckDataNodes = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func,
    children: PropTypes.node,
  },

  contextTypes: {
    me: webUserShape,
  },

  getInitialState() {
    return {
      isFetching: true,
      // A list of 'queryable' data nodes.
      dataNodes: null,
    };
  },

  // Data nodes are considered healthy (part of the cluster and most likely
  // accepting queries) if they have a status of 'joined' and a valid http address.
  getHealthyDataNodes(dataNodes) {
    if (!dataNodes) {
      return [];
    }

    return dataNodes.filter(node => (
      node.status === 'joined' && node.httpAddr !== ''
    )).map(node => {
      const scheme = node.httpScheme ? node.httpScheme : 'http';
      return `${scheme}://${node.httpAddr}`;
    });
  },

  componentDidMount() {
    const {clusterID} = this.props.params;
    showCluster(clusterID).then((resp) => {
      const dataNodes = this.getHealthyDataNodes(resp.data.data);
      this.setState({
        dataNodes,
        isFetching: false,
      });
    }).catch((err) => {
      console.error(err); // eslint-disable-line no-console
      this.setState({isFetching: false});
    });
  },

  render() {
    if (this.state.isFetching) {
      return <div className="page-spinner" />;
    }

    const {dataNodes} = this.state;
    if (!dataNodes || !dataNodes.length) {
      return <NoClusterError />;
    }

    const {me} = this.context;
    if (!me.cluster_links || !me.cluster_links.length) {
      return <NoClusterLinksError />;
    }

    return this.props.children && React.cloneElement(this.props.children, Object.assign({}, this.props, {
      dataNodes,
    }));
  },
});

export default CheckDataNodes;
