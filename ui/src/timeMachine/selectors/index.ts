// Libraries
import memoizeOne from 'memoize-one'
import {flatMap} from 'lodash'

// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'

// Types
import {FluxTable} from 'src/types'
import {AppState} from 'src/types'
import {DashboardDraftQuery} from 'src/types/dashboards'

export const getActiveTimeMachine = (state: AppState) => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const timeMachine = timeMachines[activeTimeMachineID]

  return timeMachine
}

export const getActiveQuery = (state: AppState): DashboardDraftQuery => {
  const {draftQueries, activeQueryIndex} = getActiveTimeMachine(state)

  return draftQueries[activeQueryIndex]
}

const getTablesMemoized = memoizeOne(
  (files: string[]): FluxTable[] => (files ? flatMap(files, parseResponse) : [])
)

export const getTables = (state: AppState): FluxTable[] =>
  getTablesMemoized(getActiveTimeMachine(state).queryResults.files)
