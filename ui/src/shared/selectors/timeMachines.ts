import {AppState, DashboardQuery, Source} from 'src/types/v2'

import {getSources} from 'src/sources/selectors'

export const getActiveTimeMachine = (state: AppState) => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const timeMachine = timeMachines[activeTimeMachineID]

  return timeMachine
}

export const getActiveQuery = (state: AppState): DashboardQuery => {
  const {draftQueries, activeQueryIndex} = getActiveTimeMachine(state)

  return draftQueries[activeQueryIndex]
}

export const getActiveQuerySource = (state: AppState): Source => {
  // We only support the self source for now, but in the future the active
  // query may be using some other source or the “dynamic source”
  return getSources(state).find(s => s.type === Source.TypeEnum.Self)
}
