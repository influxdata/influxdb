// Libraries
import memoizeOne from 'memoize-one'
import {get} from 'lodash'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'
import {VariableValues, ValueSelections} from 'src/variables/types'

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

export const getValueSelections = (
  state: AppState,
  contextID: string
): ValueSelections => {
  const contextValues: VariableValues = get(
    state,
    `variables.values.${contextID}.values`,
    {}
  )

  const selections: ValueSelections = Object.keys(contextValues).reduce(
    (acc, k) => {
      const selectedValue = get(contextValues, `${k}.selectedValue`)

      if (!selectedValue) {
        return acc
      }

      return {...acc, [k]: selectedValue}
    },
    {}
  )

  return selections
}
