import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {connect} from 'react-redux';
import {getSources} from 'src/shared/apis';
import {loadSources as loadSourcesAction} from 'src/shared/actions/sources';
import {showDatabases} from 'src/shared/apis/metaQuery';

const {bool, number, string, node, func, shape} = PropTypes;

// Acts as a 'router middleware'. The main `App` component is responsible for
// getting the list of data nodes, but not every page requires them to function.
// Routes that do require data nodes can be nested under this component.
const CheckSources = React.createClass({
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
    sources: PropTypes.array.isRequired,
    loadSourcesAction: PropTypes.func.isRequired,
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
    };
  },

  componentDidMount() {
    getSources().then(({data: {sources}}) => {
      this.props.loadSourcesAction(sources);
      this.setState({isFetching: false});
    }).catch(() => {
      this.props.addFlashMessage({type: 'error', text: "Unable to connect to Chronograf server"});
      this.setState({isFetching: false});
    });
  },

  componentWillUpdate(nextProps, nextState) {
    const {router, location, params, addFlashMessage, sources} = nextProps;
    const {isFetching} = nextState;
    const source = sources.find((s) => s.id === params.sourceID);
    if (!isFetching && !source) {
      return router.push(`/sources/new?redirectPath=${location.pathname}`);
    }

    if (!isFetching && !location.pathname.includes("/manage-sources")) {
      // Do simple query to proxy to see if the source is up.
      showDatabases(source.links.proxy).catch(() => {
        addFlashMessage({type: 'error', text: `Unable to connect to source`});
      });
    }
  },

  render() {
    const {params, sources} = this.props;
    const {isFetching} = this.state;
    const source = sources.find((s) => s.id === params.sourceID);

    if (isFetching || !source) {
      return <div className="page-spinner" />;
    }

    return this.props.children && React.cloneElement(this.props.children, Object.assign({}, this.props, {
      source,
    }));
  },
});

function mapStateToProps(state) {
  return {
    sources: state.sources,
  };
}

export default connect(mapStateToProps, {loadSourcesAction})(withRouter(CheckSources));
