import 'babel-polyfill'

import React, {PureComponent} from 'react'
import {render} from 'react-dom'
import {Provider} from 'react-redux'
import {Router, Route, useRouterHistory} from 'react-router'
import {createHistory} from 'history'
import {syncHistoryWithStore} from 'react-router-redux'
import {bindActionCreators} from 'redux'

import configureStore from 'src/store/configureStore'
import {loadLocalStorage} from 'src/localStorage'

import {getRootNode} from 'src/utils/nodes'
import {getBasepath} from 'src/utils/basepath'

import App from 'src/App'
import CheckSources from 'src/CheckSources'
import {StatusPage} from 'src/status'
import {DashboardsPage, DashboardPage} from 'src/dashboards'
import {LogsPage} from 'src/logs'
import {SourcePage, ManageSources} from 'src/sources'
import {FluxPage} from 'src/flux'
import NotFound from 'src/shared/components/NotFound'

import {getLinksAsync} from 'src/shared/actions/links'

import {disablePresentationMode} from 'src/shared/actions/app'
import {errorThrown} from 'src/shared/actions/errors'
import {notify} from 'src/shared/actions/notifications'

import 'src/style/chronograf.scss'

import * as ErrorsModels from 'src/types/errors'

const errorsQueue = []

const rootNode = getRootNode()

const basepath = getBasepath()

declare global {
  interface Window {
    basepath: string
  }
}

// Older method used for pre-IE 11 compatibility
window.basepath = basepath

const browserHistory = useRouterHistory(createHistory)({
  basename: basepath, // this is written in when available by the URL prefixer middleware
})

const store = configureStore(loadLocalStorage(errorsQueue), browserHistory)
const {dispatch} = store

browserHistory.listen(() => {
  dispatch(disablePresentationMode())
})

window.addEventListener('keyup', event => {
  const escapeKeyCode = 27
  // fallback for browsers that don't support event.key
  if (event.key === 'Escape' || event.keyCode === escapeKeyCode) {
    dispatch(disablePresentationMode())
  }
})

const history = syncHistoryWithStore(browserHistory, store)

interface State {
  ready: boolean
}

class Root extends PureComponent<{}, State> {
  private getLinks = bindActionCreators(getLinksAsync, dispatch)

  constructor(props) {
    super(props)
    this.state = {
      ready: false,
    }
  }

  public async componentDidMount() {
    this.flushErrorsQueue()

    try {
      await this.getLinks()
      this.setState({ready: true})
    } catch (error) {
      dispatch(errorThrown(error))
    }
  }

  public render() {
    return this.state.ready ? (
      <Provider store={store}>
        <Router history={history}>
          <Route path="/" component={CheckSources} />
          <Route component={App}>
            <Route path="/logs" component={LogsPage} />
          </Route>
          <Route path="/sources/new" component={SourcePage} />
          <Route path="/sources/:sourceID" component={App}>
            <Route component={CheckSources}>
              <Route path="status" component={StatusPage} />
              <Route path="dashboards" component={DashboardsPage} />
              <Route path="dashboards/:dashboardID" component={DashboardPage} />
              <Route path="manage-sources" component={ManageSources} />
              <Route path="manage-sources/new" component={SourcePage} />
              <Route path="manage-sources/:id/edit" component={SourcePage} />
              <Route path="delorean" component={FluxPage} />
            </Route>
          </Route>
          <Route path="*" component={NotFound} />
        </Router>
      </Provider>
    ) : (
      <div className="page-spinner" />
    )
  }

  private flushErrorsQueue() {
    if (errorsQueue.length) {
      errorsQueue.forEach(error => {
        if (typeof error === 'object') {
          dispatch(notify(error))
        } else {
          dispatch(
            errorThrown(
              {status: 0, auth: null},
              error,
              ErrorsModels.AlertType.Warning
            )
          )
        }
      })
    }
  }
}

if (rootNode) {
  render(<Root />, rootNode)
}
