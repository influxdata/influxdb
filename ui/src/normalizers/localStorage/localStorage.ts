// Libraries
import {get, omit} from 'lodash'

// Types
import {LocalStorage} from 'src/types'
import {FlagState} from 'src/shared/reducers/flags'

// Constants
import {VERSION} from 'src/shared/constants'

// Utils
import {
  timeMachines,
  getLocalStateRanges,
  setLocalStateRanges,
  normalizeApp,
} from 'src/normalizers/localStorage'
import {normalizeResources} from './resources'
import {TimeMachineState} from 'src/timeMachine/reducers'

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

  newState.timeMachines = timeMachines(state)

  return newState
}

export const normalizeSetLocalStorage = (state: LocalStorage): LocalStorage => {
  const {app, flags, ranges, autoRefresh, userSettings, timeMachines} = state

  return {
    VERSION,
    autoRefresh,
    userSettings,
    flags: {override: flags.override} as FlagState,
    app: normalizeApp(app),
    ranges: setLocalStateRanges(ranges),
    resources: normalizeResources(state),
    timeMachines: {
      activeTimeMachineID: timeMachines.activeTimeMachineID,
      timeMachines: {
        de: omit(
          timeMachines.timeMachines.de,
          'queryResults.files'
        ) as TimeMachineState,
      },
    },
  }
}
