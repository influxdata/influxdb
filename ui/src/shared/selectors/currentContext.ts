import {AppState} from 'src/types'

// NOTE: this selector is dependant on the timemachine
// and currentDashboard stores being defined
export const currentContext = (state: AppState): string => {
  if (state.currentDashboard.id) {
    return state.currentDashboard.id
  }

  return ''

  const tmID = state.timeMachines.activeTimeMachineID

  if (tmID) {
    if (state.timeMachines.timeMachines[tmID].contextID) {
      return state.timeMachines.timeMachines[tmID].contextID
    }

    return tmID
  }

  return ''
}
