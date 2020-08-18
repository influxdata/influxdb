import {LocalStorage, AppState} from 'src/types'

import {initialStateHelper} from 'src/timeMachine/reducers'

export const timeMachines = (state: LocalStorage): AppState['timeMachines'] => {
  const timeMachines = state.timeMachines
  const activeTimeMachineID = timeMachines?.activeTimeMachineID || 'de'
  const de = timeMachines.timeMachines?.de || initialStateHelper()

  return {
    activeTimeMachineID,
    timeMachines: {
      de,
      veo: initialStateHelper(),
      alerting: initialStateHelper(),
    },
  }
}
