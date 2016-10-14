import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {getSources} from 'shared/apis';

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
      sources: [],
    };
  },

  componentDidMount() {
    getSources().then(({data: {sources}}) => {
      this.setState({sources, isFetching: false});
    }).catch((err) => {
      console.error(err); // eslint-disable-line no-console
      this.setState({isFetching: false});
    });
  },

  componentWillUpdate(nextProps, nextState) {
    const {router, location, params} = nextProps;
    const {isFetching, sources} = nextState;
    if (!isFetching && !sources.find((s) => s.id === params.sourceID)) {
      return router.push(`/?redirectPath=${location.pathname}`);
    }
  },

  render() {
    const {params} = this.props;
    const {isFetching, sources} = this.state;
    const source = sources.find((s) => s.id === params.sourceID);

    if (isFetching || !source) {
      return <div className="page-spinner" />;
    }

    // Can't this be done with context? I am very confused by this. This seems
    // like dependecy injection to me- Kevin
    return this.props.children && React.cloneElement(this.props.children, Object.assign({}, this.props, {
      source,
    }));
  },
});

export default withRouter(CheckDataNodes);
