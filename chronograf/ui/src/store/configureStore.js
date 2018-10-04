import {createStore, applyMiddleware, compose} from 'redux'
import {combineReducers} from 'redux'
import {routerReducer, routerMiddleware} from 'react-router-redux'
import thunkMiddleware from 'redux-thunk'

import {resizeLayout} from 'src/shared/middleware/resizeLayout'
import {queryStringConfig} from 'src/shared/middleware/queryStringConfig'
import sharedReducers from 'src/shared/reducers'
import persistStateEnhancer from './persistStateEnhancer'
import scriptReducer from 'src/flux/reducers/script'
import sourceReducer from 'src/sources/reducers/sources'
import setupReducer from 'src/onboarding/reducers/setup'

// v2 reducers
import rangesReducer from 'src/dashboards/reducers/v2/ranges'
import dashboardsReducer from 'src/dashboards/reducers/v2/dashboards'
import hoverTimeReducer from 'src/dashboards/reducers/v2/hoverTime'
import activeViewReducer from 'src/dashboards/reducers/v2/views'
import logsReducer from 'src/logs/reducers'

const rootReducer = combineReducers({
  ...sharedReducers,
  ranges: rangesReducer,
  hoverTime: hoverTimeReducer,
  dashboards: dashboardsReducer,
  routing: routerReducer,
  script: scriptReducer,
  sources: sourceReducer,
  isSetupComplete: setupReducer,
  activeViewID: activeViewReducer,
  logs: logsReducer,
})

const composeEnhancers = window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose

export default function configureStore(initialState, browserHistory) {
  const routingMiddleware = routerMiddleware(browserHistory)
  const createPersistentStore = composeEnhancers(
    persistStateEnhancer(),
    applyMiddleware(
      thunkMiddleware,
      routingMiddleware,
      queryStringConfig,
      resizeLayout
    )
  )(createStore)

  // https://github.com/elgerlambert/redux-localstorage/issues/42
  // createPersistantStore should ONLY take reducer and initialState
  // any store enhancers must be added to the compose() function.
  return createPersistentStore(rootReducer, initialState)
}
