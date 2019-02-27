// Libraries
import memoizeOne from 'memoize-one'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'

type VariablesState = AppState['variables']['variables']

const getVariablesForOrgMemoized = memoizeOne(
  (variablesState: VariablesState, orgID: string) => {
    return Object.values(variablesState)
      .filter(
        d => d.status === RemoteDataState.Done && d.variable.orgID === orgID
      )
      .map(d => d.variable)
  }
)

export const getVariablesForOrg = (state: AppState, orgID: string) => {
  return getVariablesForOrgMemoized(state.variables.variables, orgID)
}
