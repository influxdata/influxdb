import 'babel-polyfill'
import 'abortcontroller-polyfill/dist/polyfill-patch-fetch'

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
import DataExplorerPage from 'src/dataExplorer/components/DataExplorerPage'
import {MePage} from 'src/me'
import NotFound from 'src/shared/components/NotFound'
import GetLinks from 'src/shared/containers/GetLinks'
import GetMe from 'src/shared/containers/GetMe'
import UnauthenticatedApp from 'src/shared/containers/UnauthenticatedApp'
import EditVEO from 'src/dashboards/components/EditVEO'
import NewVEO from 'src/dashboards/components/NewVEO'
import NoteEditorOverlay from 'src/dashboards/components/NoteEditorOverlay'
import OnboardingWizardPage from 'src/onboarding/containers/OnboardingWizardPage'
import BucketsIndex from 'src/buckets/containers/BucketsIndex'
import TemplatesIndex from 'src/templates/containers/TemplatesIndex'
import TelegrafsPage from 'src/telegrafs/containers/TelegrafsPage'
import ClientLibrariesPage from 'src/clientLibraries/containers/ClientLibrariesPage'
import VariablesIndex from 'src/variables/containers/VariablesIndex'
import ScrapersIndex from 'src/scrapers/containers/ScrapersIndex'
import SetOrg from 'src/shared/containers/SetOrg'
import RouteToOrg from 'src/shared/containers/RouteToOrg'
import CreateScraperOverlay from 'src/scrapers/components/CreateScraperOverlay'
import TokensIndex from 'src/authorizations/containers/TokensIndex'
import MembersIndex from 'src/members/containers/MembersIndex'
import LabelsIndex from 'src/labels/containers/LabelsIndex'
import TelegrafConfigOverlay from 'src/telegrafs/components/TelegrafConfigOverlay'
import LineProtocolWizard from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import TelegrafInstructionsOverlay from 'src/telegrafs/components/TelegrafInstructionsOverlay'
import OrgProfilePage from 'src/organizations/containers/OrgProfilePage'
import AlertingIndex from 'src/alerting/components/AlertingIndex'
import AlertHistoryIndex from 'src/alerting/components/AlertHistoryIndex'

import {FeatureFlag} from 'src/shared/utils/featureFlag'

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

export const store = configureStore(loadLocalStorage(), history)
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
                <Route component={UnauthenticatedApp}>
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
                        <Route path=":orgID" component={SetOrg}>
                          <IndexRoute component={MePage} />
                          <Route path="tasks" component={TasksPage} />
                          <Route
                            path="tasks/:id/runs"
                            component={TaskRunsPage}
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
                          <Route
                            path="dashboards/:dashboardID"
                            component={DashboardPage}
                          >
                            <Route path="cells">
                              <Route path="new" component={NewVEO} />
                              <Route path=":cellID/edit" component={EditVEO} />
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
                          <Route path="load-data">
                            <IndexRoute component={BucketsIndex} />
                            <Route path="tokens" component={TokensIndex} />
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
                              <Route path="new" component={CollectorsWizard} />
                            </Route>
                            <Route path="scrapers" component={ScrapersIndex}>
                              <Route
                                path="new"
                                component={CreateScraperOverlay}
                              />
                            </Route>
                            <FeatureFlag name="clientLibrariesPage">
                              <Route
                                path="client-libraries"
                                component={ClientLibrariesPage}
                              />
                            </FeatureFlag>
                          </Route>
                          <Route path="settings">
                            <IndexRoute component={MembersIndex} />
                            <Route path="members" component={MembersIndex} />
                            <Route
                              path="templates"
                              component={TemplatesIndex}
                            />
                            <Route
                              path="variables"
                              component={VariablesIndex}
                            />
                            <Route path="labels" component={LabelsIndex} />
                            <Route path="profile" component={OrgProfilePage} />
                          </Route>
                          <Route path="alerting" component={AlertingIndex} />
                          <Route
                            path="alert-history"
                            component={AlertHistoryIndex}
                          />
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
