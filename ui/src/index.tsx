import 'babel-polyfill'

import React, {PureComponent} from 'react'
import {render} from 'react-dom'
import {Provider} from 'react-redux'
import {Router, Route, useRouterHistory, IndexRoute} from 'react-router'
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
import SigninPage from 'src/onboarding/containers/SigninPage'
import Logout from 'src/Logout'
import TaskPage from 'src/tasks/containers/TaskPage'
import TasksPage from 'src/tasks/containers/TasksPage'
import TaskRunsPage from 'src/tasks/components/TaskRunsPage'
import OrganizationsIndex from 'src/organizations/containers/OrganizationsIndex'
import OrganizationView from 'src/organizations/containers/OrganizationView'
import OrgTaskPage from 'src/organizations/components/OrgTaskPage'
import OrgTaskEditPage from 'src/organizations/components/OrgTaskEditPage'
import TaskEditPage from 'src/tasks/containers/TaskEditPage'
import DashboardPage from 'src/dashboards/components/DashboardPage'
import DashboardsIndex from 'src/dashboards/components/dashboard_index/DashboardsIndex'
import DataExplorerPage from 'src/dataExplorer/components/DataExplorerPage'
import {MePage, Account} from 'src/me'
import NotFound from 'src/shared/components/NotFound'
import GetLinks from 'src/shared/containers/GetLinks'
import GetMe from 'src/shared/containers/GetMe'
import SourcesPage from 'src/sources/components/SourcesPage'
import ConfigurationPage from 'src/configuration/components/ConfigurationPage'

import OnboardingWizardPage from 'src/onboarding/containers/OnboardingWizardPage'

// Actions
import {disablePresentationMode} from 'src/shared/actions/app'

// Styles
import 'src/style/chronograf.scss'
import '@influxdata/clockface/dist/index.css'

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
              <Route path="/onboarding">
                <Route path=":stepID" component={OnboardingWizardPage} />
                <Route
                  path=":stepID/:substepID"
                  component={OnboardingWizardPage}
                />
              </Route>
              <Route path="/signin" component={SigninPage} />
              <Route path="/logout" component={Logout} />
              <Route path="/">
                <Route component={Signin}>
                  <Route component={GetMe}>
                    <Route component={GetOrganizations}>
                      <Route component={App}>
                        <Route component={GetSources}>
                          <Route component={SetActiveSource}>
                            <IndexRoute component={MePage} />
                            <Route path="organizations">
                              <IndexRoute component={OrganizationsIndex} />
                              <Route path=":orgID">
                                <Route
                                  path="tasks_tab/new"
                                  component={OrgTaskPage}
                                />
                                <Route
                                  path="tasks_tab/:id"
                                  component={OrgTaskEditPage}
                                />
                                <Route
                                  path=":tab"
                                  component={OrganizationView}
                                />
                              </Route>
                            </Route>
                            <Route path="tasks">
                              <IndexRoute component={TasksPage} />
                              <Route path=":id/runs" component={TaskRunsPage} />
                              <Route path="new" component={TaskPage} />
                              <Route path=":id" component={TaskEditPage} />
                            </Route>
                            <Route
                              path="data-explorer"
                              component={DataExplorerPage}
                            />
                            <Route path="dashboards">
                              <IndexRoute component={DashboardsIndex} />
                              <Route
                                path=":dashboardID"
                                component={DashboardPage}
                              />
                            </Route>
                            <Route path="me" component={MePage} />
                            <Route path="account/:tab" component={Account} />
                            <Route path="sources" component={SourcesPage} />
                            <Route
                              path="configuration/:tab"
                              component={ConfigurationPage}
                            />
                          </Route>
                        </Route>
                      </Route>
                    </Route>
                  </Route>
                </Route>
              </Route>
            </Route>
          </Route>
          <Route path="*" component={NotFound} />
        </Router>
      </Provider>
    )
  }
}

if (rootNode) {
  render(<Root />, rootNode)
}
