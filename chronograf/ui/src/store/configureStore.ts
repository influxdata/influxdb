import {createStore, applyMiddleware, compose} from 'redux'
import {History} from 'history'
import {combineReducers, Store} from 'redux'
import {routerReducer, routerMiddleware} from 'react-router-redux'
import thunkMiddleware from 'redux-thunk'

import {resizeLayout} from 'src/shared/middleware/resizeLayout'
import {queryStringConfig} from 'src/shared/middleware/queryStringConfig'
import sharedReducers from 'src/shared/reducers'
import persistStateEnhancer from './persistStateEnhancer'
import sourcesReducer from 'src/sources/reducers/sources'

// v2 reducers
import meReducer from 'src/shared/reducers/v2/me'
import tasksReducer from 'src/tasks/reducers/v2'
import rangesReducer from 'src/dashboards/reducers/v2/ranges'
import dashboardsReducer from 'src/dashboards/reducers/v2/dashboards'
import hoverTimeReducer from 'src/dashboards/reducers/v2/hoverTime'
import viewsReducer from 'src/dashboards/reducers/v2/views'
import logsReducer from 'src/logs/reducers'
import timeMachinesReducer from 'src/shared/reducers/v2/timeMachines'
import orgsReducer from 'src/organizations/reducers/orgs'
import sourceReducer from 'src/shared/reducers/v2/source'

// Types
import {LocalStorage} from 'src/types/localStorage'
import {AppState} from 'src/types/v2'

type ReducerState = Pick<
  AppState,
  Exclude<keyof AppState, 'VERSION' | 'timeRange'>
>

const rootReducer = combineReducers<ReducerState>({
  ...sharedReducers,
  ranges: rangesReducer,
  hoverTime: hoverTimeReducer,
  dashboards: dashboardsReducer,
  timeMachines: timeMachinesReducer,
  routing: routerReducer,
  sources: sourcesReducer,
  views: viewsReducer,
  logs: logsReducer,
  tasks: tasksReducer,
  orgs: orgsReducer,
  me: meReducer,
  source: sourceReducer,
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
