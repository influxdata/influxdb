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
import {
  Login,
  UserIsAuthenticated,
  UserIsNotAuthenticated,
  Purgatory,
} from 'src/auth'
import CheckSources from 'src/CheckSources'
import {StatusPage} from 'src/status'
import {HostsPage, HostPage} from 'src/hosts'
import DataExplorerPage from 'src/data_explorer'
import {DashboardsPage, DashboardPage} from 'src/dashboards'
import AlertsApp from 'src/alerts'
import {
  KapacitorPage,
  KapacitorRulePage,
  KapacitorRulesPage,
  TickscriptPage,
} from 'src/kapacitor'
import {AdminChronografPage, AdminInfluxDBPage} from 'src/admin'
import {SourcePage, ManageSources} from 'src/sources'
import {IFQLPage} from 'src/ifql'
import NotFound from 'src/shared/components/NotFound'

import {getLinksAsync} from 'src/shared/actions/links'
import {getMeAsync} from 'src/shared/actions/auth'

import {disablePresentationMode} from 'src/shared/actions/app'
import {errorThrown} from 'src/shared/actions/errors'
import {notify} from 'src/shared/actions/notifications'

import 'src/style/chronograf.scss'

import {HEARTBEAT_INTERVAL} from 'src/shared/constants'

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
  private getMe = bindActionCreators(getMeAsync, dispatch)

  constructor(props) {
    super(props)
    this.state = {
      ready: false,
    }
  }

  public async componentWillMount() {
    this.flushErrorsQueue()

    try {
      await this.getLinks()
      await this.checkAuth()
      this.setState({ready: true})
    } catch (error) {
      dispatch(errorThrown(error))
    }
  }

  public render() {
    return this.state.ready ? (
      <Provider store={store}>
        <Router history={history}>
          <Route path="/" component={UserIsAuthenticated(CheckSources)} />
          <Route path="/login" component={UserIsNotAuthenticated(Login)} />
          <Route path="/purgatory" component={UserIsAuthenticated(Purgatory)} />
          <Route
            path="/sources/new"
            component={UserIsAuthenticated(SourcePage)}
          />
          <Route path="/sources/:sourceID" component={UserIsAuthenticated(App)}>
            <Route component={CheckSources}>
              <Route path="status" component={StatusPage} />
              <Route path="hosts" component={HostsPage} />
              <Route path="hosts/:hostID" component={HostPage} />
              <Route
                path="chronograf/data-explorer"
                component={DataExplorerPage}
              />
              <Route path="dashboards" component={DashboardsPage} />
              <Route path="dashboards/:dashboardID" component={DashboardPage} />
              <Route path="alerts" component={AlertsApp} />
              <Route path="alert-rules" component={KapacitorRulesPage} />
              <Route path="alert-rules/:ruleID" component={KapacitorRulePage} />
              <Route path="alert-rules/new" component={KapacitorRulePage} />
              <Route path="tickscript/new" component={TickscriptPage} />
              <Route path="tickscript/:ruleID" component={TickscriptPage} />
              <Route path="kapacitors/new" component={KapacitorPage} />
              <Route path="kapacitors/:id/edit" component={KapacitorPage} />
              <Route
                path="kapacitors/:id/edit:hash"
                component={KapacitorPage}
              />
              <Route
                path="admin-chronograf/:tab"
                component={AdminChronografPage}
              />
              <Route path="admin-influxdb/:tab" component={AdminInfluxDBPage} />
              <Route path="manage-sources" component={ManageSources} />
              <Route path="manage-sources/new" component={SourcePage} />
              <Route path="manage-sources/:id/edit" component={SourcePage} />
              <Route path="delorean" component={IFQLPage} />
            </Route>
          </Route>
          <Route path="*" component={NotFound} />
        </Router>
      </Provider>
    ) : (
      <div className="page-spinner" />
    )
  }

  private async performHeartbeat({shouldResetMe = false} = {}) {
    await this.getMe({shouldResetMe})

    setTimeout(() => {
      if (store.getState().auth.me !== null) {
        this.performHeartbeat()
      }
    }, HEARTBEAT_INTERVAL)
  }

  private flushErrorsQueue() {
    if (errorsQueue.length) {
      errorsQueue.forEach(error => {
        if (typeof error === 'object') {
          dispatch(notify(error))
        } else {
          dispatch(errorThrown({status: 0, auth: null}, error, 'warning'))
        }
      })
    }
  }

  private async checkAuth() {
    try {
      await this.performHeartbeat({shouldResetMe: true})
    } catch (error) {
      dispatch(errorThrown(error))
    }
  }
}

if (rootNode) {
  render(<Root />, rootNode)
}
