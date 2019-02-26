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
import GetOrganizations from 'src/shared/containers/GetOrganizations'
import Setup from 'src/Setup'
import Signin from 'src/Signin'
import SigninPage from 'src/onboarding/containers/SigninPage'
import Logout from 'src/Logout'
import TaskPage from 'src/tasks/containers/TaskPage'
import TasksPage from 'src/tasks/containers/TasksPage'
import TaskRunsPage from 'src/tasks/components/TaskRunsPage'
import OrganizationsIndex from 'src/organizations/containers/OrganizationsIndex'
import OrgTaskPage from 'src/organizations/components/OrgTaskPage'
import OrgTaskEditPage from 'src/organizations/components/OrgTaskEditPage'
import OrgBucketIndex from 'src/organizations/containers/OrgBucketsIndex'
import TaskEditPage from 'src/tasks/containers/TaskEditPage'
import DashboardPage from 'src/dashboards/components/DashboardPage'
import DashboardsIndex from 'src/dashboards/components/dashboard_index/DashboardsIndex'
import DataExplorerPage from 'src/dataExplorer/components/DataExplorerPage'
import {MePage, Account} from 'src/me'
import NotFound from 'src/shared/components/NotFound'
import GetLinks from 'src/shared/containers/GetLinks'
import GetMe from 'src/shared/containers/GetMe'
import ConfigurationPage from 'src/configuration/components/ConfigurationPage'
import OrgDashboardsIndex from 'src/organizations/containers/OrgDashboardsIndex'
import OrgMembersIndex from 'src/organizations/containers/OrgMembersIndex'
import OrgTelegrafsIndex from 'src/organizations/containers/OrgTelegrafsIndex'
import OrgVariablesIndex from 'src/organizations/containers/OrgVariablesIndex'
import OrgScrapersIndex from 'src/organizations/containers/OrgScrapersIndex'
import OrgTasksIndex from 'src/organizations/containers/OrgTasksIndex'

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
                        <IndexRoute component={MePage} />
                        <Route path="organizations">
                          <IndexRoute component={OrganizationsIndex} />
                          <Route path=":orgID">
                            <Route path="tasks/new" component={OrgTaskPage} />
                            <Route
                              path="tasks/:id"
                              component={OrgTaskEditPage}
                            />
                            <Route path="buckets" component={OrgBucketIndex} />
                            <Route
                              path="dashboards"
                              component={OrgDashboardsIndex}
                            />
                            <Route path="members" component={OrgMembersIndex} />
                            <Route
                              path="telegrafs"
                              component={OrgTelegrafsIndex}
                            />
                            <Route
                              path="variables"
                              component={OrgVariablesIndex}
                            />
                            <Route
                              path="scrapers"
                              component={OrgScrapersIndex}
                            />
                            <Route path="tasks" component={OrgTasksIndex} />
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
          <Route path="*" component={NotFound} />
        </Router>
      </Provider>
    )
  }
}

if (rootNode) {
  render(<Root />, rootNode)
}
