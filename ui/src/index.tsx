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
import DashboardCreateFromTemplateOverlay from 'src/dashboards/components/createFromTemplateOverlay/DashboardCreateFromTemplateOverlay'
import CreateVariableOverlay from 'src/variables/components/CreateVariableOverlay'
import DataExplorerPage from 'src/dataExplorer/components/DataExplorerPage'
import SaveAsOverlay from 'src/dataExplorer/components/SaveAsOverlay'
import {MePage, Account} from 'src/me'
import NotFound from 'src/shared/components/NotFound'
import GetLinks from 'src/shared/containers/GetLinks'
import GetMe from 'src/shared/containers/GetMe'
import Notifications from 'src/shared/containers/Notifications'
import TaskExportOverlay from 'src/tasks/components/TaskExportOverlay'
import TaskImportOverlay from 'src/tasks/components/TaskImportOverlay'
import VEO from 'src/dashboards/components/VEO'
import NoteEditorOverlay from 'src/dashboards/components/NoteEditorOverlay'
import OnboardingWizardPage from 'src/onboarding/containers/OnboardingWizardPage'
import BucketsIndex from 'src/buckets/containers/BucketsIndex'
import TemplatesIndex from 'src/templates/containers/TemplatesIndex'
import TelegrafsPage from 'src/telegrafs/containers/TelegrafsPage'
import TemplateImportOverlay from 'src/templates/components/TemplateImportOverlay'
import TemplateExportOverlay from 'src/templates/components/TemplateExportOverlay'
import VariablesIndex from 'src/variables/containers/VariablesIndex'
import ScrapersIndex from 'src/scrapers/containers/ScrapersIndex'
import VariableImportOverlay from 'src/variables/components/VariableImportOverlay'
import VariableExportOverlay from 'src/variables/components/VariableExportOverlay'
import SetOrg from 'src/shared/containers/SetOrg'
import RouteToOrg from 'src/shared/containers/RouteToOrg'
import CreateOrgOverlay from 'src/organizations/components/CreateOrgOverlay'
import CreateScraperOverlay from 'src/scrapers/components/CreateScraperOverlay'
import TokensIndex from 'src/authorizations/containers/TokensIndex'
import MembersIndex from 'src/members/containers/MembersIndex'
import LabelsIndex from 'src/labels/containers/LabelsIndex'
import TemplateViewOverlay from 'src/templates/components/TemplateViewOverlay'
import TelegrafConfigOverlay from 'src/telegrafs/components/TelegrafConfigOverlay'
import LineProtocolWizard from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import TelegrafInstructionsOverlay from 'src/telegrafs/components/TelegrafInstructionsOverlay'
import AddMembersOverlay from 'src/members/components/AddMembersOverlay'
import OrgProfilePage from 'src/organizations/containers/OrgProfilePage'
import RenameOrgOverlay from 'src/organizations/components/RenameOrgOverlay'
import UpdateBucketOverlay from 'src/buckets/components/UpdateBucketOverlay'
import RenameBucketOverlay from 'src/buckets/components/RenameBucketOverlay'
import RenameVariableOverlay from 'src/variables/components/RenameVariableOverlay'
import UpdateVariableOverlay from 'src/variables/components/UpdateVariableOverlay'
import AllAccessTokenOverlay from 'src/authorizations/components/AllAccessTokenOverlay'
import BucketsTokenOverlay from 'src/authorizations/components/BucketsTokenOverlay'

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
                              component={DashboardCreateFromTemplateOverlay}
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
                          <Route path="settings">
                            <IndexRoute component={MembersIndex} />
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
                                path="rename"
                                component={RenameBucketOverlay}
                              />
                            </Route>
                          </Route>
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
                          <Route path="members" component={MembersIndex}>
                            <Route path="new" component={AddMembersOverlay} />
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
                            <Route path="new" component={CollectorsWizard} />
                          </Route>
                          <Route path="templates" component={TemplatesIndex}>
                            <Route
                              path="import"
                              component={TemplateImportOverlay}
                            />
                            <Route
                              path=":id/export"
                              component={TemplateExportOverlay}
                            />
                            <Route
                              path=":id/view"
                              component={TemplateViewOverlay}
                            />
                          </Route>
                          <Route path="variables" component={VariablesIndex}>
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
                          <Route path="labels" component={LabelsIndex} />
                          <Route path="scrapers" component={ScrapersIndex}>
                            <Route
                              path="new"
                              component={CreateScraperOverlay}
                            />
                          </Route>
                          <Route path="profile" component={OrgProfilePage}>
                            <Route path="rename" component={RenameOrgOverlay} />
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
