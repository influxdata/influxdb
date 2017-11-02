import 'babel-polyfill'

import React from 'react'
import {render} from 'react-dom'
import {Provider} from 'react-redux'
import {Router, Route, useRouterHistory} from 'react-router'
import {createHistory} from 'history'
import {syncHistoryWithStore} from 'react-router-redux'

import configureStore from 'src/store/configureStore'
import {loadLocalStorage} from 'src/localStorage'

import App from 'src/App'
import {Login, UserIsAuthenticated, UserIsNotAuthenticated} from 'src/auth'
import CheckSources from 'src/CheckSources'
import {StatusPage} from 'src/status'
import {HostsPage, HostPage} from 'src/hosts'
import DataExplorer from 'src/data_explorer'
import {DashboardsPage, DashboardPage} from 'src/dashboards'
import AlertsApp from 'src/alerts'
import {
  KapacitorPage,
  KapacitorRulePage,
  KapacitorRulesPage,
  KapacitorTasksPage,
  TickscriptPage,
} from 'src/kapacitor'
import {AdminChronografPage, AdminInfluxDBPage} from 'src/admin'
import {SourcePage, ManageSources} from 'src/sources'
import NotFound from 'shared/components/NotFound'

import {getMe} from 'shared/apis'

import {disablePresentationMode} from 'shared/actions/app'
import {
  authRequested,
  authReceived,
  meRequested,
  meReceived,
  logoutLinkReceived,
} from 'shared/actions/auth'
import {linksReceived} from 'shared/actions/links'
import {errorThrown} from 'shared/actions/errors'

import 'src/style/chronograf.scss'

import {HEARTBEAT_INTERVAL} from 'shared/constants'

const errorsQueue = []

const rootNode = document.getElementById('react-root')

// Older method used for pre-IE 11 compatibility
const basepath = rootNode.getAttribute('data-basepath') || ''
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

const Root = React.createClass({
  componentWillMount() {
    this.flushErrorsQueue()
    this.checkAuth()
  },

  async checkAuth() {
    dispatch(authRequested())
    dispatch(meRequested())
    try {
      await this.startHeartbeat({shouldDispatchResponse: true})
    } catch (error) {
      dispatch(errorThrown(error))
    }
  },

  async startHeartbeat({shouldDispatchResponse}) {
    try {
      // These non-me objects are added to every response by some AJAX trickery
      const {data: me, auth, logoutLink, external, users} = await getMe()
      if (shouldDispatchResponse) {
        dispatch(authReceived(auth))
        dispatch(meReceived(me))
        dispatch(logoutLinkReceived(logoutLink))
        dispatch(linksReceived({external, users}))
      }

      setTimeout(() => {
        if (store.getState().auth.me !== null) {
          this.startHeartbeat({shouldDispatchResponse: false})
        }
      }, HEARTBEAT_INTERVAL)
    } catch (error) {
      dispatch(errorThrown(error))
    }
  },

  flushErrorsQueue() {
    if (errorsQueue.length) {
      errorsQueue.forEach(errorText => {
        dispatch(errorThrown({status: 0, auth: null}, errorText, 'warning'))
      })
    }
  },

  render() {
    return (
      <Provider store={store}>
        <Router history={history}>
          <Route path="/" component={UserIsAuthenticated(CheckSources)} />
          <Route path="/login" component={UserIsNotAuthenticated(Login)} />
          <Route
            path="/sources/new"
            component={UserIsAuthenticated(SourcePage)}
          />
          <Route path="/sources/:sourceID" component={UserIsAuthenticated(App)}>
            <Route component={CheckSources}>
              <Route path="status" component={StatusPage} />
              <Route path="hosts" component={HostsPage} />
              <Route path="hosts/:hostID" component={HostPage} />
              <Route path="chronograf/data-explorer" component={DataExplorer} />
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
              <Route path="kapacitor-tasks" component={KapacitorTasksPage} />
              <Route path="admin-chronograf" component={AdminChronografPage} />
              <Route path="admin-influxdb" component={AdminInfluxDBPage} />
              <Route path="manage-sources" component={ManageSources} />
              <Route path="manage-sources/new" component={SourcePage} />
              <Route path="manage-sources/:id/edit" component={SourcePage} />
            </Route>
          </Route>
          <Route path="*" component={NotFound} />
        </Router>
      </Provider>
    )
  },
})

if (rootNode) {
  render(<Root />, rootNode)
}
