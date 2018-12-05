import {AppState, DashboardQuery} from 'src/types/v2'

export const getActiveTimeMachine = (state: AppState) => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const timeMachine = timeMachines[activeTimeMachineID]

  return timeMachine
}

export const getActiveQuery = (state: AppState): DashboardQuery => {
  const {view, activeQueryIndex} = getActiveTimeMachine(state)

  return view.properties.queries[activeQueryIndex]
}

export const getActiveDraftScript = (state: AppState) => {
  const {draftScripts, activeQueryIndex} = getActiveTimeMachine(state)
  const activeDraftScript = draftScripts[activeQueryIndex]

  return activeDraftScript
}
