import 'babel-polyfill'
import 'abortcontroller-polyfill/dist/polyfill-patch-fetch'

import React, {PureComponent} from 'react'
import {render} from 'react-dom'
import {Provider} from 'react-redux'
import {Route} from 'react-router-dom'
import {ConnectedRouter} from 'connected-react-router'

// import {CLOUD} from 'src/shared/constants'
import configureStore, {history} from 'src/store/configureStore'
import {loadLocalStorage} from 'src/localStorage'

import {getRootNode} from 'src/utils/nodes'
import {getBrowserBasepath} from 'src/utils/basepath'
import {updateReportingContext} from 'src/cloud/utils/reporting'

// Components
import Setup from 'src/Setup'
import NotFound from 'src/shared/components/NotFound'
import GetLinks from 'src/shared/containers/GetLinks'

// Utilities
import {writeNavigationTimingMetrics} from 'src/cloud/utils/rum'

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
          <Route component={Setup} />
          <Route component={NotFound} />
          {/*<Route component={Setup}>
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
