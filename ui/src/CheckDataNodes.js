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
      source: null,
    };
  },

  componentDidMount() {
    getSources().then(({data: {sources}}) => {
      this.setState({
        source: sources[0],
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

    const {source} = this.state;
    if (!source) {
      // this should probably be changed....
      return <NoClusterError />;
    }

    return this.props.children && React.cloneElement(this.props.children, Object.assign({}, this.props, {
      source,
    }));
  },
});

export default CheckDataNodes;
