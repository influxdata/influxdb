import {createStore, applyMiddleware, compose} from 'redux'
import {History, createBrowserHistory} from 'history'
import {combineReducers, Store} from 'redux'
import {connectRouter, routerMiddleware} from 'connected-react-router'
import thunkMiddleware from 'redux-thunk'

import {resizeLayout} from 'src/shared/middleware/resizeLayout'
import {queryStringConfig} from 'src/shared/middleware/queryStringConfig'
import sharedReducers from 'src/shared/reducers'
import persistStateEnhancer from './persistStateEnhancer'

// v2 reducers
import meReducer from 'src/shared/reducers/me'
import flagReducer from 'src/shared/reducers/flags'
import currentDashboardReducer from 'src/shared/reducers/currentDashboard'
import currentPageReducer from 'src/shared/reducers/currentPage'
import tasksReducer from 'src/tasks/reducers'
import rangesReducer from 'src/dashboards/reducers/ranges'
import {dashboardsReducer} from 'src/dashboards/reducers/dashboards'
import {cellsReducer} from 'src/cells/reducers'
import viewsReducer from 'src/views/reducers'
import {timeMachinesReducer} from 'src/timeMachine/reducers'
import {orgsReducer} from 'src/organizations/reducers'
import overlaysReducer from 'src/overlays/reducers/overlays'
import onboardingReducer from 'src/onboarding/reducers'
import noteEditorReducer from 'src/dashboards/reducers/notes'
import dataLoadingReducer from 'src/dataLoaders/reducers'
import {variablesReducer, variableEditorReducer} from 'src/variables/reducers'
import {labelsReducer} from 'src/labels/reducers'
import {bucketsReducer} from 'src/buckets/reducers'
import {telegrafsReducer} from 'src/telegrafs/reducers'
import {authsReducer} from 'src/authorizations/reducers'
import templatesReducer from 'src/templates/reducers'
import {scrapersReducer} from 'src/scrapers/reducers'
import {userSettingsReducer} from 'src/userSettings/reducers'
import {membersReducer} from 'src/members/reducers'
import {autoRefreshReducer} from 'src/shared/reducers/autoRefresh'
import {limitsReducer, LimitsState} from 'src/cloud/reducers/limits'
import {demoDataReducer, DemoDataState} from 'src/cloud/reducers/demodata'
import {
  orgSettingsReducer,
  OrgSettingsState,
} from 'src/cloud/reducers/orgsettings'
import checksReducer from 'src/checks/reducers'
import rulesReducer from 'src/notifications/rules/reducers'
import endpointsReducer from 'src/notifications/endpoints/reducers'
import {
  pluginsReducer,
  activePluginsReducer,
  editorReducer,
  pluginsResourceReducer,
} from 'src/dataLoaders/reducers/telegrafEditor'
import {predicatesReducer} from 'src/shared/reducers/predicates'
import alertBuilderReducer from 'src/alerting/reducers/alertBuilder'

// Types
import {AppState, LocalStorage} from 'src/types'
import {queryCacheReducer} from 'src/queryCache/reducers'

type ReducerState = Pick<AppState, Exclude<keyof AppState, 'timeRange'>>

export const history = createBrowserHistory()

export const rootReducer = (history: History) =>
  combineReducers<ReducerState>({
    router: connectRouter(history),
    ...sharedReducers,
    autoRefresh: autoRefreshReducer,
    alertBuilder: alertBuilderReducer,
    cloud: combineReducers<{
      limits: LimitsState
      demoData: DemoDataState
      orgSettings: OrgSettingsState
    }>({
      limits: limitsReducer,
      demoData: demoDataReducer,
      orgSettings: orgSettingsReducer,
    }),
    currentPage: currentPageReducer,
    currentDashboard: currentDashboardReducer,
    dataLoading: dataLoadingReducer,
    me: meReducer,
    flags: flagReducer,
    noteEditor: noteEditorReducer,
    onboarding: onboardingReducer,
    overlays: overlaysReducer,
    plugins: pluginsResourceReducer,
    predicates: predicatesReducer,
    queryCache: queryCacheReducer,
    ranges: rangesReducer,
    resources: combineReducers({
      buckets: bucketsReducer,
      cells: cellsReducer,
      checks: checksReducer,
      dashboards: dashboardsReducer,
      endpoints: endpointsReducer,
      labels: labelsReducer,
      members: membersReducer,
      orgs: orgsReducer,
      rules: rulesReducer,
      scrapers: scrapersReducer,
      tasks: tasksReducer,
      telegrafs: telegrafsReducer,
      templates: templatesReducer,
      tokens: authsReducer,
      variables: variablesReducer,
      views: viewsReducer,
    }),
    telegrafEditor: editorReducer,
    telegrafEditorActivePlugins: activePluginsReducer,
    telegrafEditorPlugins: pluginsReducer,
    timeMachines: timeMachinesReducer,
    userSettings: userSettingsReducer,
    variableEditor: variableEditorReducer,
    VERSION: () => '',
  })

const composeEnhancers =
  (window as any).__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose

let _store

// NOTE: just used to reset between tests
export function clearStore() {
  _store = null
}

export default function configureStore(
  initialState?: LocalStorage
): Store<AppState & LocalStorage> {
  // NOTE: memoizing helps keep singular instances of the store
  // after initialization, or else actions start failing for reasons
  if (_store) {
    return _store
  }

  const routingMiddleware = routerMiddleware(history)
  const createPersistentStore = composeEnhancers(
    persistStateEnhancer(),
    applyMiddleware(
      thunkMiddleware,
      routingMiddleware,
      resizeLayout,
      queryStringConfig
    )
  )(createStore)

  // https://github.com/elgerlambert/redux-localstorage/issues/42
  // createPersistentStore should ONLY take reducer and initialState
  // any store enhancers must be added to the compose() function.
  _store = createPersistentStore(rootReducer(history), initialState)
  return _store
}
