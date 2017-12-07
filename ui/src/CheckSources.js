import React, {Component, PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {
  isUserAuthorized,
  VIEWER_ROLE,
  EDITOR_ROLE,
  ADMIN_ROLE,
} from 'src/auth/Authorized'

import {showDatabases} from 'shared/apis/metaQuery'

import {getSourcesAsync} from 'shared/actions/sources'
import {errorThrown as errorThrownAction} from 'shared/actions/errors'
import {publishNotification} from 'shared/actions/notifications'

import {DEFAULT_HOME_PAGE} from 'shared/constants'

// Acts as a 'router middleware'. The main `App` component is responsible for
// getting the list of data nodes, but not every page requires them to function.
// Routes that do require data nodes can be nested under this component.
class CheckSources extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isFetching: true,
    }
  }

  getChildContext() {
    const {sources, params: {sourceID}} = this.props
    return {source: sources.find(s => s.id === sourceID)}
  }

  async componentWillMount() {
    const {auth: {isUsingAuth, me}} = this.props

    if (!isUsingAuth || isUserAuthorized(me.role, VIEWER_ROLE)) {
      await this.props.getSources()
    }

    this.setState({isFetching: false})
  }

  shouldComponentUpdate(nextProps) {
    const {auth: {isUsingAuth, me}} = nextProps
    // don't update this component if currentOrganization is what has changed,
    // or else the app will try to call showDatabases in componentWillUpdate,
    // which will fail unless sources have been refreshed
    if (
      isUsingAuth &&
      me.currentOrganization.id !== this.props.auth.me.currentOrganization.id
    ) {
      return false
    }
    return true
  }

  async componentWillUpdate(nextProps, nextState) {
    const {
      router,
      location,
      params,
      errorThrown,
      sources,
      auth: {isUsingAuth, me},
      notify,
      getSources,
    } = nextProps
    const {isFetching} = nextState
    const source = sources.find(s => s.id === params.sourceID)
    const defaultSource = sources.find(s => s.default === true)

    if (
      isUserAuthorized(this.props.auth.me.role, ADMIN_ROLE) &&
      !isUserAuthorized(nextProps.auth.me.role, ADMIN_ROLE)
    ) {
      return router.push('/')
    }

    if (!isFetching && isUsingAuth && !isUserAuthorized(me.role, VIEWER_ROLE)) {
      // if you're a member, go to purgatory.
      return router.push('/purgatory')
    }

    // TODO: At this point, the sources we have in Redux may be out of sync with what's on the server
    // Do we need to refresh this data more frequently? Does it need to come as frequently as the `me` response?
    if (!isFetching && !source) {
      const rest = location.pathname.match(/\/sources\/\d+?\/(.+)/)
      const restString = rest === null ? DEFAULT_HOME_PAGE : rest[1]

      if (isUsingAuth && !isUserAuthorized(me.role, EDITOR_ROLE)) {
        if (defaultSource) {
          return router.push(`/sources/${defaultSource.id}/${restString}`)
        } else if (sources[0]) {
          return router.push(`/sources/${sources[0].id}/${restString}`)
        }
        // if you're a viewer and there are no sources, go to purgatory.
        notify('error', 'Organization has no sources configured')
        return router.push('/purgatory')
      }

      // if you're an editor or not using auth, try for sources or otherwise
      // create one
      if (defaultSource) {
        return router.push(`/sources/${defaultSource.id}/${restString}`)
      } else if (sources[0]) {
        return router.push(`/sources/${sources[0].id}/${restString}`)
      }

      return router.push(`/sources/new?redirectPath=${location.pathname}`)
    }

    if (!isFetching && !location.pathname.includes('/manage-sources')) {
      // Do simple query to proxy to see if the source is up.
      try {
        // the guard around currentOrganization prevents this showDatabases
        // invocation since sources haven't been refreshed yet
        await showDatabases(source.links.proxy)
      } catch (error) {
        try {
          const newSources = await getSources()
          if (newSources.length) {
            errorThrown(
              error,
              `Source ${source.name} is no longer available. Successfully connected to another source.`
            )
          } else {
            errorThrown(
              error,
              `Unable to connect to source ${source.name}. No other sources available.`
            )
          }
        } catch (error2) {
          errorThrown(error2, 'Unable to retrieve sources')
        }
      }
    }
  }

  render() {
    const {
      params,
      sources,
      auth: {isUsingAuth, me: {currentOrganization}},
    } = this.props
    const {isFetching} = this.state
    const source = sources.find(s => s.id === params.sourceID)

    if (isFetching || !source || (isUsingAuth && !currentOrganization)) {
      return <div className="page-spinner" />
    }

    // TODO: guard against invalid resource access

    return (
      this.props.children &&
      React.cloneElement(
        this.props.children,
        Object.assign({}, this.props, {
          source,
        })
      )
    )
  }
}

const {arrayOf, bool, func, node, shape, string} = PropTypes

CheckSources.propTypes = {
  getSources: func.isRequired,
  sources: arrayOf(
    shape({
      links: shape({
        proxy: string.isRequired,
        self: string.isRequired,
        kapacitors: string.isRequired,
        queries: string.isRequired,
        permissions: string.isRequired,
        users: string.isRequired,
        databases: string.isRequired,
      }).isRequired,
    })
  ),
  children: node,
  params: shape({
    sourceID: string,
  }).isRequired,
  router: shape({
    push: func.isRequired,
  }).isRequired,
  location: shape({
    pathname: string.isRequired,
  }).isRequired,
  auth: shape({
    isUsingAuth: bool,
    me: shape({
      currentOrganization: shape({
        name: string.isRequired,
        id: string.isRequired,
      }),
    }),
  }),
  notify: func.isRequired,
}

CheckSources.childContextTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
      self: string.isRequired,
      kapacitors: string.isRequired,
      queries: string.isRequired,
      permissions: string.isRequired,
      users: string.isRequired,
      databases: string.isRequired,
    }).isRequired,
  }),
}

const mapStateToProps = ({sources, auth}) => ({
  sources,
  auth,
})

const mapDispatchToProps = dispatch => ({
  getSources: bindActionCreators(getSourcesAsync, dispatch),
  errorThrown: bindActionCreators(errorThrownAction, dispatch),
  notify: bindActionCreators(publishNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(
  withRouter(CheckSources)
)
