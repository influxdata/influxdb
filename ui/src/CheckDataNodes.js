import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {getSources} from 'src/shared/apis';

const {bool, number, string, node, func, shape} = PropTypes;

// Acts as a 'router middleware'. The main `App` component is responsible for
// getting the list of data nodes, but not every page requires them to function.
// Routes that do require data nodes can be nested under this component.
const CheckDataNodes = React.createClass({
  propTypes: {
    addFlashMessage: func,
    children: node,
    params: PropTypes.shape({
      sourceID: PropTypes.string,
    }).isRequired,
    router: PropTypes.shape({
      push: PropTypes.func.isRequired,
    }).isRequired,
    location: PropTypes.shape({
      pathname: PropTypes.string.isRequired,
    }).isRequired,
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
    const {router, location, params} = this.props;

    getSources().then(({data: {sources}}) => {
      const source = sources.find((s) => s.id === params.sourceID);

      if (!source) { // would be great to check source.status or similar here, or try run a query against the source
        return router.push(`/?redirectPath=${location.pathname}`);
      }

      this.setState({
        source,
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

    return this.props.children && React.cloneElement(this.props.children, Object.assign({}, this.props, {
      source: this.state.source,
    }));
  },
});

export default withRouter(CheckDataNodes);
