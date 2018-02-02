import 'babel-polyfill'

import React from 'react'
import {render} from 'react-dom'
import {Provider} from 'react-redux'
import {Router, Route, useRouterHistory} from 'react-router'
import {createHistory} from 'history'
import {syncHistoryWithStore} from 'react-router-redux'
import {bindActionCreators} from 'redux'

import configureStore from 'src/store/configureStore'
import {loadLocalStorage} from 'src/localStorage'

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

import {getLinksAsync} from 'shared/actions/links'
import {getMeAsync} from 'shared/actions/auth'

import {disablePresentationMode} from 'shared/actions/app'
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
  async componentWillMount() {
    this.flushErrorsQueue()

    try {
      await this.getLinks()
      this.checkAuth()
    } catch (error) {
      dispatch(errorThrown(error))
    }
  },

  getLinks: bindActionCreators(getLinksAsync, dispatch),
  getMe: bindActionCreators(getMeAsync, dispatch),

  async checkAuth() {
    try {
      await this.performHeartbeat({shouldResetMe: true})
    } catch (error) {
      dispatch(errorThrown(error))
    }
  },

  async performHeartbeat({shouldResetMe = false} = {}) {
    await this.getMe({shouldResetMe})

    setTimeout(() => {
      if (store.getState().auth.me !== null) {
        this.performHeartbeat()
      }
    }, HEARTBEAT_INTERVAL)
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
              <Route
                path="kapacitors/:id/edit:hash"
                component={KapacitorPage}
              />
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
