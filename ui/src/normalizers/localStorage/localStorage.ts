// Libraries
import {get} from 'lodash'

// Types
import {LocalStorage} from 'src/types'

// Constants
import {VERSION} from 'src/shared/constants'

// Utils
import {
  getLocalStateRanges,
  setLocalStateRanges,
  normalizeApp,
} from 'src/normalizers/localStorage'
import {normalizeResources} from './resources'

export const normalizeGetLocalStorage = (state: LocalStorage): LocalStorage => {
  let newState = state

  if (state.ranges) {
    newState = {...newState, ranges: getLocalStateRanges(state.ranges)}
  }

  const appPersisted = get(newState, 'app.persisted', false)
  if (appPersisted) {
    newState = {
      ...newState,
      app: normalizeApp(newState.app),
    }
  }

  return newState
}

export const normalizeSetLocalStorage = (state: LocalStorage): LocalStorage => {
  const {app, ranges, autoRefresh, userSettings} = state
  return {
    VERSION,
    autoRefresh,
    userSettings,
    app: normalizeApp(app),
    ranges: setLocalStateRanges(ranges),
    resources: normalizeResources(state),
  }
}
