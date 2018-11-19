import 'babel-polyfill'

import React, {PureComponent} from 'react'
import {render} from 'react-dom'
import {Provider} from 'react-redux'
import {Router, Route, useRouterHistory} from 'react-router'
import {createHistory, History} from 'history'

import configureStore from 'src/store/configureStore'
import {loadLocalStorage} from 'src/localStorage'

import {getRootNode} from 'src/utils/nodes'
import {getBasepath} from 'src/utils/basepath'

// Components
import App from 'src/App'
import GetSources from 'src/shared/containers/GetSources'
import SetActiveSource from 'src/shared/containers/SetActiveSource'
import GetOrganizations from 'src/shared/containers/GetOrganizations'
import Setup from 'src/Setup'
import Signin from 'src/Signin'
import TaskPage from 'src/tasks/containers/TaskPage'
import TasksPage from 'src/tasks/containers/TasksPage'
import OrganizationsIndex from 'src/organizations/containers/OrganizationsIndex'
import OrganizationView from 'src/organizations/containers/OrganizationView'
import TaskEditPage from 'src/tasks/containers/TaskEditPage'
import DashboardPage from 'src/dashboards/components/DashboardPage'
import DashboardsIndex from 'src/dashboards/components/dashboard_index/DashboardsIndex'
import DataExplorerPage from 'src/dataExplorer/components/DataExplorerPage'
import {UserPage} from 'src/user'
import {LogsPage} from 'src/logs'
import NotFound from 'src/shared/components/NotFound'
import GetLinks from 'src/shared/containers/GetLinks'
import GetMe from 'src/shared/containers/GetMe'
import SourcesPage from 'src/sources/components/SourcesPage'

// Actions
import {disablePresentationMode} from 'src/shared/actions/app'

// Styles
import 'src/style/chronograf.scss'

const rootNode = getRootNode()
const basepath = getBasepath()

declare global {
  interface Window {
    basepath: string
  }
}

// Older method used for pre-IE 11 compatibility
window.basepath = basepath

const history: History = useRouterHistory(createHistory)({
  basename: basepath, // this is written in when available by the URL prefixer middleware
})

const store = configureStore(loadLocalStorage(), history)
const {dispatch} = store

history.listen(() => {
  dispatch(disablePresentationMode())
})

window.addEventListener('keyup', event => {
  const escapeKeyCode = 27
  // fallback for browsers that don't support event.key
  if (event.key === 'Escape' || event.keyCode === escapeKeyCode) {
    dispatch(disablePresentationMode())
  }
})

class Root extends PureComponent {
  public render() {
    return (
      <Provider store={store}>
        <Router history={history}>
          <Route component={GetLinks}>
            <Route component={Setup}>
              <Route component={Signin}>
                <Route component={GetMe}>
                  <Route component={GetOrganizations}>
                    <Route component={App}>
                      <Route path="/" component={GetSources}>
                        <Route path="/" component={SetActiveSource}>
                          <Route
                            path="dashboards/:dashboardID"
                            component={DashboardPage}
                          />
                          <Route path="tasks" component={TasksPage} />
                          <Route
                            path="organizations"
                            component={OrganizationsIndex}
                          />
                          <Route
                            path="organizations/:orgID/:tab"
                            component={OrganizationView}
                          />
                          <Route path="tasks/new" component={TaskPage} />
                          <Route path="tasks/:id" component={TaskEditPage} />
                          <Route
                            path="data-explorer"
                            component={DataExplorerPage}
                          />
                          <Route
                            path="dashboards"
                            component={DashboardsIndex}
                          />
                          <Route path="user_profile" component={UserPage} />
                          <Route path="logs" component={LogsPage} />
                          <Route path="sources" component={SourcesPage} />
                        </Route>
                      </Route>
                    </Route>
                  </Route>
                </Route>
              </Route>
            </Route>
            <Route path="*" component={NotFound} />
          </Route>
        </Router>
      </Provider>
    )
  }
}

if (rootNode) {
  render(<Root />, rootNode)
}
