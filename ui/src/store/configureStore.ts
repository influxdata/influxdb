import {createStore, applyMiddleware, compose} from 'redux'
import {History} from 'history'
import {combineReducers, Store} from 'redux'
import {routerReducer, routerMiddleware} from 'react-router-redux'
import thunkMiddleware from 'redux-thunk'

import {resizeLayout} from 'src/shared/middleware/resizeLayout'
import {queryStringConfig} from 'src/shared/middleware/queryStringConfig'
import sharedReducers from 'src/shared/reducers'
import persistStateEnhancer from './persistStateEnhancer'

// v2 reducers
import meReducer from 'src/shared/reducers/me'
import tasksReducer from 'src/tasks/reducers'
import rangesReducer from 'src/dashboards/reducers/ranges'
import {dashboardsReducer} from 'src/dashboards/reducers/dashboards'
import viewsReducer from 'src/dashboards/reducers/views'
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
import checksReducer from 'src/alerting/reducers/checks'
import rulesReducer from 'src/alerting/reducers/notifications/rules'
import endpointsReducer from 'src/alerting/reducers/notifications/endpoints'
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

type ReducerState = Pick<AppState, Exclude<keyof AppState, 'timeRange'>>

export const rootReducer = combineReducers<ReducerState>({
  ...sharedReducers,
  autoRefresh: autoRefreshReducer,
  alertBuilder: alertBuilderReducer,
  checks: checksReducer,
  cloud: combineReducers<{limits: LimitsState}>({limits: limitsReducer}),
  dashboards: dashboardsReducer,
  dataLoading: dataLoadingReducer,
  endpoints: endpointsReducer,
  labels: labelsReducer,
  me: meReducer,
  noteEditor: noteEditorReducer,
  onboarding: onboardingReducer,
  overlays: overlaysReducer,
  plugins: pluginsResourceReducer,
  predicates: predicatesReducer,
  ranges: rangesReducer,
  resources: combineReducers({
    buckets: bucketsReducer,
    members: membersReducer,
    orgs: orgsReducer,
    scrapers: scrapersReducer,
    tokens: authsReducer,
    telegrafs: telegrafsReducer,
  }),
  routing: routerReducer,
  rules: rulesReducer,
  tasks: tasksReducer,
  telegrafEditor: editorReducer,
  telegrafEditorActivePlugins: activePluginsReducer,
  telegrafEditorPlugins: pluginsReducer,
  templates: templatesReducer,
  timeMachines: timeMachinesReducer,
  userSettings: userSettingsReducer,
  variables: variablesReducer,
  variableEditor: variableEditorReducer,
  views: viewsReducer,
  VERSION: () => '',
})

const composeEnhancers =
  (window as any).__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose

export default function configureStore(
  initialState: LocalStorage,
  history: History
): Store<AppState & LocalStorage> {
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
  return createPersistentStore(rootReducer, initialState)
}
