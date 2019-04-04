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
import TaskEditPage from 'src/tasks/containers/TaskEditPage'
import DashboardPage from 'src/dashboards/components/DashboardPage'
import DashboardsIndex from 'src/dashboards/components/dashboard_index/DashboardsIndex'
import DashboardExportOverlay from 'src/dashboards/components/DashboardExportOverlay'
import DashboardImportOverlay from 'src/dashboards/components/DashboardImportOverlay'
import DashboardImportFromTemplateOverlay from 'src/dashboards/components/DashboardImportFromTemplateOverlay'
import DataExplorerPage from 'src/dataExplorer/components/DataExplorerPage'
import SaveAsOverlay from 'src/dataExplorer/components/SaveAsOverlay'
import {MePage, Account} from 'src/me'
import NotFound from 'src/shared/components/NotFound'
import GetLinks from 'src/shared/containers/GetLinks'
import GetMe from 'src/shared/containers/GetMe'
import Notifications from 'src/shared/containers/Notifications'
import ConfigurationPage from 'src/configuration/components/ConfigurationPage'
import TaskExportOverlay from 'src/tasks/components/TaskExportOverlay'
import TaskImportOverlay from 'src/tasks/components/TaskImportOverlay'
import VEO from 'src/dashboards/components/VEO'
import NoteEditorOverlay from 'src/dashboards/components/NoteEditorOverlay'
import OnboardingWizardPage from 'src/onboarding/containers/OnboardingWizardPage'
import BucketsIndex from 'src/buckets/containers/BucketsIndex'
import OrgMembersIndex from 'src/organizations/containers/OrgMembersIndex'
import OrgTelegrafsIndex from 'src/organizations/containers/OrgTelegrafsIndex'
import OrgTemplatesIndex from 'src/organizations/containers/OrgTemplatesIndex'
import TemplateImportOverlay from 'src/templates/components/TemplateImportOverlay'
import TemplateExportOverlay from 'src/templates/components/TemplateExportOverlay'
import OrgVariablesIndex from 'src/organizations/containers/OrgVariablesIndex'
import OrgScrapersIndex from 'src/organizations/containers/OrgScrapersIndex'
import VariableImportOverlay from 'src/variables/components/VariableImportOverlay'
import OrgVariableExportOverlay from 'src/organizations/components/OrgVariableExportOverlay'
import SetOrg from 'src/shared/containers/SetOrg'
import RouteToOrg from 'src/shared/containers/RouteToOrg'

import TokensIndex from 'src/authorizations/containers/TokensIndex'

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
                <Route component={Notifications}>
                  <Route path="/signin" component={SigninPage} />
                  <Route path="/logout" component={Logout} />
                </Route>
              </Route>
              <Route component={Signin}>
                <Route component={GetMe}>
                  <Route component={GetOrganizations}>
                    <Route path="/">
                      <IndexRoute component={RouteToOrg} />
                      <Route path="orgs/:orgID" component={App}>
                        <Route component={SetOrg}>
                          <IndexRoute component={MePage} />
                          <Route path="tasks" component={TasksPage}>
                            <Route
                              path=":id/export"
                              component={TaskExportOverlay}
                            />
                            <Route
                              path="import"
                              component={TaskImportOverlay}
                            />
                          </Route>
                          <Route
                            path="tasks/:id/runs"
                            component={TaskRunsPage}
                          />
                          <Route path="tasks/new" component={TaskPage} />
                          <Route path="tasks/:id" component={TaskEditPage} />
                          <Route
                            path="data-explorer"
                            component={DataExplorerPage}
                          >
                            <Route path="save" component={SaveAsOverlay} />
                          </Route>
                          <Route path="dashboards" component={DashboardsIndex}>
                            <Route
                              path="import"
                              component={DashboardImportOverlay}
                            />
                            <Route
                              path="import/template"
                              component={DashboardImportFromTemplateOverlay}
                            />
                            <Route
                              path=":dashboardID/export"
                              component={DashboardExportOverlay}
                            />
                          </Route>
                          <Route
                            path="dashboards/:dashboardID"
                            component={DashboardPage}
                          >
                            <Route path="cells">
                              <Route path="new" component={VEO} />
                              <Route path=":cellID/edit" component={VEO} />
                            </Route>
                            <Route path="notes">
                              <Route path="new" component={NoteEditorOverlay} />
                              <Route
                                path=":cellID/edit"
                                component={NoteEditorOverlay}
                              />
                            </Route>
                          </Route>
                          <Route path="me" component={MePage} />
                          <Route path="account/:tab" component={Account} />
                          <Route
                            path="configuration/:tab"
                            component={ConfigurationPage}
                          />
                          <Route path="settings">
                            <IndexRoute component={OrgMembersIndex} />
                          </Route>
                          <Route path="buckets" component={BucketsIndex} />
                          <Route path="tokens" component={TokensIndex} />
                          <Route path="members" component={OrgMembersIndex} />
                          <Route
                            path="telegrafs"
                            component={OrgTelegrafsIndex}
                          />
                          <Route path="templates" component={OrgTemplatesIndex}>
                            <Route
                              path="import"
                              component={TemplateImportOverlay}
                            />
                            <Route
                              path=":id/export"
                              component={TemplateExportOverlay}
                            />
                          </Route>
                          <Route path="variables" component={OrgVariablesIndex}>
                            <Route
                              path="import"
                              component={VariableImportOverlay}
                            />
                            <Route
                              path=":id/export"
                              component={OrgVariableExportOverlay}
                            />
                          </Route>
                          <Route path="scrapers" component={OrgScrapersIndex} />
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
