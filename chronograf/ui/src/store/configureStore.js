import {createStore, applyMiddleware, compose} from 'redux'
import {combineReducers} from 'redux'
import {routerReducer, routerMiddleware} from 'react-router-redux'
import thunkMiddleware from 'redux-thunk'

import {resizeLayout} from 'src/shared/middleware/resizeLayout'
import {queryStringConfig} from 'src/shared/middleware/queryStringConfig'
import statusReducers from 'src/status/reducers'
import sharedReducers from 'src/shared/reducers'
import cellEditorOverlay from 'src/dashboards/reducers/cellEditorOverlay'
import persistStateEnhancer from './persistStateEnhancer'
import scriptReducer from 'src/flux/reducers/script'
import sourceReducer from 'src/sources/reducers/sources'

// v2 reducers
import rangesReducer from 'src/dashboards/reducers/v2/ranges'
import dashboardsReducer from 'src/dashboards/reducers/v2/dashboards'
import hoverTimeReducer from 'src/dashboards/reducers/v2/hoverTime'
import activeViewReducer from 'src/dashboards/reducers/v2/views'

const rootReducer = combineReducers({
  ...statusReducers,
  ...sharedReducers,
  cellEditorOverlay,
  ranges: rangesReducer,
  hoverTime: hoverTimeReducer,
  dashboards: dashboardsReducer,
  routing: routerReducer,
  script: scriptReducer,
  sources: sourceReducer,
  activeViewID: activeViewReducer,
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
