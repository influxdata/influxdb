import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {getSources} from 'shared/apis'
import {loadSources as loadSourcesAction} from 'shared/actions/sources'
import {showDatabases} from 'shared/apis/metaQuery'
import {publishNotification as publishNotificationAction} from 'shared/actions/notifications'

// Acts as a 'router middleware'. The main `App` component is responsible for
// getting the list of data nodes, but not every page requires them to function.
// Routes that do require data nodes can be nested under this component.
const CheckSources = React.createClass({
  propTypes: {
    children: PropTypes.node,
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
    notify: PropTypes.func.isRequired,
    loadSources: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      isFetching: true,
    }
  },

  async componentWillMount() {
    const {loadSources, notify} = this.props

    try {
      const {data: {sources}} = await getSources()
      loadSources(sources)
      this.setState({isFetching: false})
    } catch (error) {
      notify('error', 'Unable to connect to Chronograf server')
      this.setState({isFetching: false})
    }
  },

  componentWillUpdate(nextProps, nextState) {
    const {router, location, params, notify, sources} = nextProps
    const {isFetching} = nextState
    const source = sources.find((s) => s.id === params.sourceID)
    const defaultSource = sources.find((s) => s.default === true)

    if (!isFetching && !source) {
      const rest = location.pathname.match(/\/sources\/\d+?\/(.+)/)
      const restString = rest === null ? 'hosts' : rest[1]

      if (defaultSource) {
        return router.push(`/sources/${defaultSource.id}/${restString}`)
      } else if (sources[0]) {
        return router.push(`/sources/${sources[0].id}/${restString}`)
      }

      return router.push(`/sources/new?redirectPath=${location.pathname}`)
    }

    if (!isFetching && !location.pathname.includes("/manage-sources")) {
      // Do simple query to proxy to see if the source is up.
      showDatabases(source.links.proxy).catch(() => {
        notify('error', 'Unable to connect to source')
      })
    }
  },

  render() {
    const {params, sources} = this.props
    const {isFetching} = this.state
    const source = sources.find((s) => s.id === params.sourceID)

    if (isFetching || !source) {
      return <div className="page-spinner" />
    }

    return this.props.children && React.cloneElement(this.props.children, Object.assign({}, this.props, {
      source,
    }))
  },
})

const mapStateToProps = ({sources}) => ({
  sources,
})

const mapDispatchToProps = (dispatch) => ({
  loadSources: bindActionCreators(loadSourcesAction, dispatch),
  notify: bindActionCreators(publishNotificationAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(withRouter(CheckSources))
