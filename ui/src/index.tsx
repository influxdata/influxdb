import 'babel-polyfill'
import 'abortcontroller-polyfill/dist/polyfill-patch-fetch'

import React, {PureComponent} from 'react'
import {render} from 'react-dom'
import {Provider} from 'react-redux'
import {Route, Switch} from 'react-router-dom'
import {ConnectedRouter} from 'connected-react-router'

// import {CLOUD} from 'src/shared/constants'
import configureStore, {history} from 'src/store/configureStore'
import {loadLocalStorage} from 'src/localStorage'

import {getRootNode} from 'src/utils/nodes'
import {getBrowserBasepath} from 'src/utils/basepath'
import {updateReportingContext} from 'src/cloud/utils/reporting'

// Components
import Setup from 'src/Setup'
// import App from 'src/App'
// import GetOrganizations from 'src/shared/containers/GetOrganizations'
// import SigninPage from 'src/onboarding/containers/SigninPage'
// import {LoginPage} from 'src/onboarding/containers/LoginPage'
// import Logout from 'src/Logout'
// import TaskPage from 'src/tasks/containers/TaskPage'
// import TasksPage from 'src/tasks/containers/TasksPage'
// import TaskRunsPage from 'src/tasks/components/TaskRunsPage'
// import TaskEditPage from 'src/tasks/containers/TaskEditPage'
// import DashboardContainer from 'src/dashboards/components/DashboardContainer'
// import DashboardsIndex from 'src/dashboards/components/dashboard_index/DashboardsIndex'
// import DashboardExportOverlay from 'src/dashboards/components/DashboardExportOverlay'
// import DashboardImportOverlay from 'src/dashboards/components/DashboardImportOverlay'
// import CreateFromTemplateOverlay from 'src/templates/components/createFromTemplateOverlay/CreateFromTemplateOverlay'
// import CreateVariableOverlay from 'src/variables/components/CreateVariableOverlay'
// import DataExplorerPage from 'src/dataExplorer/components/DataExplorerPage'
// import SaveAsOverlay from 'src/dataExplorer/components/SaveAsOverlay'
// import {MePage} from 'src/me'
// import NotebookPage from 'src/notebooks/components/Notebook'
import NotFound from 'src/shared/components/NotFound'
import GetLinks from 'src/shared/containers/GetLinks'
// import GetMe from 'src/shared/containers/GetMe'
// import GetFlags from 'src/shared/containers/GetFlags'
// import TaskExportOverlay from 'src/tasks/components/TaskExportOverlay'
// import TaskImportOverlay from 'src/tasks/components/TaskImportOverlay'
// import EditVEO from 'src/dashboards/components/EditVEO'
// import NewVEO from 'src/dashboards/components/NewVEO'
// import OnboardingWizardPage from 'src/onboarding/containers/OnboardingWizardPage'
// import BucketsIndex from 'src/buckets/containers/BucketsIndex'
// import TemplatesIndex from 'src/templates/containers/TemplatesIndex'
// import TelegrafsPage from 'src/telegrafs/containers/TelegrafsPage'
// import ClientLibrariesPage from 'src/clientLibraries/containers/ClientLibrariesPage'
// import ClientArduinoOverlay from 'src/clientLibraries/components/ClientArduinoOverlay'
// import ClientCSharpOverlay from 'src/clientLibraries/components/ClientCSharpOverlay'
// import ClientGoOverlay from 'src/clientLibraries/components/ClientGoOverlay'
// import ClientJavaOverlay from 'src/clientLibraries/components/ClientJavaOverlay'
// import ClientJSOverlay from 'src/clientLibraries/components/ClientJSOverlay'
// import ClientKotlinOverlay from 'src/clientLibraries/components/ClientKotlinOverlay'
// import ClientPHPOverlay from 'src/clientLibraries/components/ClientPHPOverlay'
// import ClientPythonOverlay from 'src/clientLibraries/components/ClientPythonOverlay'
// import ClientRubyOverlay from 'src/clientLibraries/components/ClientRubyOverlay'
// import ClientScalaOverlay from 'src/clientLibraries/components/ClientScalaOverlay'
// import TemplateImportOverlay from 'src/templates/components/TemplateImportOverlay'
// import TemplateExportOverlay from 'src/templates/components/TemplateExportOverlay'
// import VariablesIndex from 'src/variables/containers/VariablesIndex'
// import ScrapersIndex from 'src/scrapers/containers/ScrapersIndex'
// import VariableImportOverlay from 'src/variables/components/VariableImportOverlay'
// import VariableExportOverlay from 'src/variables/components/VariableExportOverlay'
// import SetOrg from 'src/shared/containers/SetOrg'
// import RouteToOrg from 'src/shared/containers/RouteToOrg'
// import CreateOrgOverlay from 'src/organizations/components/CreateOrgOverlay'
// import CreateScraperOverlay from 'src/scrapers/components/CreateScraperOverlay'
// import TokensIndex from 'src/authorizations/containers/TokensIndex'
// import MembersIndex from 'src/members/containers/MembersIndex'
// import LabelsIndex from 'src/labels/containers/LabelsIndex'
// import TemplateViewOverlay from 'src/templates/components/TemplateViewOverlay'
// import LineProtocolWizard from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'
// import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
// import TelegrafInstructionsOverlay from 'src/telegrafs/components/TelegrafInstructionsOverlay'
// import OrgProfilePage from 'src/organizations/containers/OrgProfilePage'
// import RenameOrgOverlay from 'src/organizations/components/RenameOrgOverlay'
// import UpdateBucketOverlay from 'src/buckets/components/UpdateBucketOverlay'
// import RenameBucketOverlay from 'src/buckets/components/RenameBucketOverlay'
// import RenameVariableOverlay from 'src/variables/components/RenameVariableOverlay'
// import UpdateVariableOverlay from 'src/variables/components/UpdateVariableOverlay'
// import TaskImportFromTemplateOverlay from 'src/tasks/components/TaskImportFromTemplateOverlay'
// import StaticTemplateViewOverlay from 'src/templates/components/StaticTemplateViewOverlay'
// import CheckHistory from 'src/checks/components/CheckHistory'
// import AlertingIndex from 'src/alerting/components/AlertingIndex'
// import AlertHistoryIndex from 'src/alerting/components/AlertHistoryIndex'
// import BucketsDeleteDataOverlay from 'src/shared/components/DeleteDataOverlay'
// import DEDeleteDataOverlay from 'src/dataExplorer/components/DeleteDataOverlay'
// import NewThresholdCheckEO from 'src/checks/components/NewThresholdCheckEO'
// import NewDeadmanCheckEO from 'src/checks/components/NewDeadmanCheckEO'
// import EditCheckEO from 'src/checks/components/EditCheckEO'
// import NewRuleOverlay from 'src/notifications/rules/components/NewRuleOverlay'
// import EditRuleOverlay from 'src/notifications/rules/components/EditRuleOverlay'
// import NewEndpointOverlay from 'src/notifications/endpoints/components/NewEndpointOverlay'
// import EditEndpointOverlay from 'src/notifications/endpoints/components/EditEndpointOverlay'
// import NoOrgsPage from 'src/organizations/containers/NoOrgsPage'

// import {CommunityTemplateImportOverlay} from 'src/templates/components/CommunityTemplateImportOverlay'

// Utilities
// import {isFlagEnabled} from 'src/shared/utils/featureFlag'
import {writeNavigationTimingMetrics} from 'src/cloud/utils/rum'

// Overlays
// import OverlayHandler, {
//   RouteOverlay,
// } from 'src/overlays/components/RouteOverlay'
// const AddNoteOverlay = RouteOverlay(
//   OverlayHandler,
//   'add-note',
//   (history, params) => {
//     history.push(`/orgs/${params.orgID}/dashboards/${params.dashboardID}`)
//   }
// )
// const EditNoteOverlay = RouteOverlay(
//   OverlayHandler,
//   'edit-note',
//   (history, params) => {
//     history.push(`/orgs/${params.orgID}/dashboards/${params.dashboardID}`)
//   }
// )
// const AllAccessTokenOverlay = RouteOverlay(
//   OverlayHandler,
//   'add-master-token',
//   (history, params) => {
//     history.push(`/orgs/${params.orgID}/load-data/tokens`)
//   }
// )
// const BucketsTokenOverlay = RouteOverlay(
//   OverlayHandler,
//   'add-token',
//   (history, params) => {
//     history.push(`/orgs/${params.orgID}/load-data/tokens`)
//   }
// )
// const TelegrafConfigOverlay = RouteOverlay(
//   OverlayHandler,
//   'telegraf-config',
//   (history, params) => {
//     history.push(`/orgs/${params.orgID}/load-data/telegrafs`)
//   }
// )
// const TelegrafOutputOverlay = RouteOverlay(
//   OverlayHandler,
//   'telegraf-output',
//   (history, params) => {
//     history.push(`/orgs/${params.orgID}/load-data/telegrafs`)
//   }
// )

// Actions
import {disablePresentationMode} from 'src/shared/actions/app'

// Styles
import 'src/style/chronograf.scss'
import '@influxdata/clockface/dist/index.css'

const rootNode = getRootNode()
const basepath = getBrowserBasepath()

const SESSION_KEY = 'session'

const cookieSession = document.cookie.match(
  new RegExp('(^| )' + SESSION_KEY + '=([^;]+)')
)

updateReportingContext({
  session: cookieSession ? cookieSession[2].slice(5) : '',
})

declare global {
  interface Window {
    basepath: string
    dataLayer: any[]
  }
}

// Older method used for pre-IE 11 compatibility
window.basepath = basepath

export const store = configureStore(loadLocalStorage())
const {dispatch} = store

if (window['Cypress']) {
  window['store'] = store
}

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
        <ConnectedRouter history={history}>
          <Route component={GetLinks} />
          <Switch>
            <Route component={Setup} />
            <Route component={NotFound} />
          </Switch>
          {/*<Route component={Setup}>
              <Route path="/onboarding">
                <Route path=":stepID" component={OnboardingWizardPage} />
                <Route
                  path=":stepID/:substepID"
                  component={OnboardingWizardPage}
                />
                </Route>
              </Route>
              <Route component={Signin}>
                <Route component={GetMe}>
                  <Route component={GetFlags}>
                    <Route component={GetOrganizations}>
                      <Route path="/">
                        <Route path="no-orgs" component={NoOrgsPage} />
                        <IndexRoute component={RouteToOrg} />
                        <Route path="orgs" component={App}>
                          <Route path="new" component={CreateOrgOverlay} />
                          <Route path=":orgID" component={SetOrg}>
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
                              <Route
                                path="import/template"
                                component={TaskImportFromTemplateOverlay}
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
                              <Route
                                path="delete-data"
                                component={DEDeleteDataOverlay}
                              />
                            </Route>
                            <Route
                              path="dashboards"
                              component={DashboardsIndex}
                            >
                              <Route
                                path="import"
                                component={DashboardImportOverlay}
                              />
                              <Route
                                path="import/template"
                                component={CreateFromTemplateOverlay}
                              />
                              <Route
                                path=":dashboardID/export"
                                component={DashboardExportOverlay}
                              />
                            </Route>
                            <Route
                              path="dashboards/:dashboardID"
                              component={DashboardContainer}
                            >
                              <Route path="cells">
                                <Route path="new" component={NewVEO} />
                                <Route
                                  path=":cellID/edit"
                                  component={EditVEO}
                                />
                              </Route>
                              <Route path="notes">
                                <Route path="new" component={AddNoteOverlay} />
                                <Route
                                  path=":cellID/edit"
                                  component={EditNoteOverlay}
                                />
                              </Route>
                            </Route>
                            <Route path="me" component={MePage} />
                            {isFlagEnabled('notebooks') && (
                              <Route
                                path="notebooks"
                                component={NotebookPage}
                              />
                            )}
                            <Route path="load-data">
                              <IndexRoute component={BucketsIndex} />
                              <Route path="tokens" component={TokensIndex}>
                                <Route path="generate">
                                  <Route
                                    path="all-access"
                                    component={AllAccessTokenOverlay}
                                  />
                                  <Route
                                    path="buckets"
                                    component={BucketsTokenOverlay}
                                  />
                                </Route>
                              </Route>
                              <Route path="buckets" component={BucketsIndex}>
                                <Route path=":bucketID">
                                  <Route
                                    path="line-protocols/new"
                                    component={LineProtocolWizard}
                                  />
                                  <Route
                                    path="telegrafs/new"
                                    component={CollectorsWizard}
                                  />
                                  <Route
                                    path="scrapers/new"
                                    component={CreateScraperOverlay}
                                  />
                                  <Route
                                    path="edit"
                                    component={UpdateBucketOverlay}
                                  />
                                  <Route
                                    path="delete-data"
                                    component={BucketsDeleteDataOverlay}
                                  />
                                  <Route
                                    path="rename"
                                    component={RenameBucketOverlay}
                                  />
                                </Route>
                              </Route>
                              <Route path="telegrafs" component={TelegrafsPage}>
                                <Route
                                  path=":id/view"
                                  component={TelegrafConfigOverlay}
                                />
                                <Route
                                  path=":id/instructions"
                                  component={TelegrafInstructionsOverlay}
                                />
                                <Route
                                  path="output"
                                  component={TelegrafOutputOverlay}
                                />
                                <Route
                                  path="new"
                                  component={CollectorsWizard}
                                />
                              </Route>
                              <Route path="scrapers" component={ScrapersIndex}>
                                <Route
                                  path="new"
                                  component={CreateScraperOverlay}
                                />
                              </Route>
                              <Route
                                path="client-libraries"
                                component={ClientLibrariesPage}
                              >
                                <Route
                                  path="arduino"
                                  component={ClientArduinoOverlay}
                                />
                                <Route
                                  path="csharp"
                                  component={ClientCSharpOverlay}
                                />
                                <Route path="go" component={ClientGoOverlay} />
                                <Route
                                  path="java"
                                  component={ClientJavaOverlay}
                                />
                                <Route
                                  path="javascript-node"
                                  component={ClientJSOverlay}
                                />
                                <Route
                                  path="kotlin"
                                  component={ClientKotlinOverlay}
                                />
                                <Route
                                  path="php"
                                  component={ClientPHPOverlay}
                                />
                                <Route
                                  path="python"
                                  component={ClientPythonOverlay}
                                />
                                <Route
                                  path="ruby"
                                  component={ClientRubyOverlay}
                                />
                                <Route
                                  path="scala"
                                  component={ClientScalaOverlay}
                                />
                              </Route>
                            </Route>
                            <Route path="settings">
                              <IndexRoute component={VariablesIndex} />
                              <Route
                                path="variables"
                                component={VariablesIndex}
                              >
                                <Route
                                  path="import"
                                  component={VariableImportOverlay}
                                />
                                <Route
                                  path=":id/export"
                                  component={VariableExportOverlay}
                                />
                                <Route
                                  path="new"
                                  component={CreateVariableOverlay}
                                />
                                <Route
                                  path=":id/rename"
                                  component={RenameVariableOverlay}
                                />
                                <Route
                                  path=":id/edit"
                                  component={UpdateVariableOverlay}
                                />
                              </Route>
                              <Route
                                path="templates"
                                component={TemplatesIndex}
                              >
                                <Route
                                  path="import"
                                  component={TemplateImportOverlay}
                                />
                                <Route
                                  path="import/:templateName"
                                  component={CommunityTemplateImportOverlay}
                                />
                                <Route
                                  path=":id/export"
                                  component={TemplateExportOverlay}
                                />
                                <Route
                                  path=":id/view"
                                  component={TemplateViewOverlay}
                                />
                                <Route
                                  path=":id/static/view"
                                  component={StaticTemplateViewOverlay}
                                />
                              </Route>
                              <Route path="labels" component={LabelsIndex} />
                              <Route path="about" component={OrgProfilePage}>
                                <Route
                                  path="rename"
                                  component={RenameOrgOverlay}
                                />
                              </Route>
                            </Route>
                            <Route path="alerting" component={AlertingIndex}>
                              <Route
                                path="checks/new-threshold"
                                component={NewThresholdCheckEO}
                              />
                              <Route
                                path="checks/new-deadman"
                                component={NewDeadmanCheckEO}
                              />
                              <Route
                                path="checks/:checkID/edit"
                                component={EditCheckEO}
                              />
                              <Route
                                path="rules/new"
                                component={NewRuleOverlay}
                              />
                              <Route
                                path="rules/:ruleID/edit"
                                component={EditRuleOverlay}
                              />
                              <Route
                                path="endpoints/new"
                                component={NewEndpointOverlay}
                              />
                              <Route
                                path="endpoints/:endpointID/edit"
                                component={EditEndpointOverlay}
                              />
                            </Route>
                            <Route
                              path="alert-history"
                              component={AlertHistoryIndex}
                            />
                            <Route
                              path="checks/:checkID"
                              component={CheckHistory}
                            />
                            <Route path="about" component={OrgProfilePage} />
                            {!CLOUD && (
                              <Route path="members" component={MembersIndex} />
                            )}
                          </Route>
                        </Route>
                      </Route>
                    </Route>
                  </Route>
                </Route>
              </Route>
                            </Route> */}
        </ConnectedRouter>
      </Provider>
    )
  }
}

if (rootNode) {
  render(<Root />, rootNode)
}

window.addEventListener('load', () => {
  writeNavigationTimingMetrics()
})
