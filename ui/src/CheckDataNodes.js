import React, {PropTypes} from 'react';
import NoClusterError from 'shared/components/NoClusterError';
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

  componentDidMount() {
    getSources().then((resp) => {
      const dataNodes = resp.data.sources;
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

    return this.props.children && React.cloneElement(this.props.children, Object.assign({}, this.props, {
      dataNodes,
    }));
  },
});

export default CheckDataNodes;
