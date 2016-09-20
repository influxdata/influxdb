import React, {PropTypes} from 'react';
import NoClusterError from 'shared/components/NoClusterError';
import NoClusterLinksError from 'shared/components/NoClusterLinksError';
import {getSources} from 'src/shared/apis';

const {bool, number, string, node, func, shape} = PropTypes;

// Acts as a 'router middleware'. The main `App` component is responsible for
// getting the list of data nodes, but not every page requires them to function.
// Routes that do require data nodes can be nested under this component.
const CheckDataNodes = React.createClass({
  propTypes: {
    addFlashMessage: func,
    children: node,
  },

  contextTypes: {
    me: shape({
      id: number.isRequired,
      name: string.isRequired,
      email: string.isRequired,
      admin: bool.isRequired,
    }),
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

    return dataNodes.filter(n => (
      n.status === 'joined' && n.httpAddr !== ''
    )).map(n => {
      const scheme = n.httpScheme ? n.httpScheme : 'http';
      return `${scheme}://${n.httpAddr}`;
    });
  },

  componentDidMount() {
    getSources().then((resp) => {
      // TODO: get this wired up correctly once getSources is working.
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
