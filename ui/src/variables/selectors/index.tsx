// Libraries
import memoizeOne from 'memoize-one'
import {get} from 'lodash'

// Utils
import {getVarAssignment} from 'src/variables/utils/getVarAssignment'

// Types
import {RemoteDataState} from 'src/types'
import {VariableAssignment} from 'src/types/ast'
import {AppState} from 'src/types/v2'
import {VariableValues, ValueSelections} from 'src/variables/types'
import {Variable} from '@influxdata/influx'

type VariablesState = AppState['variables']['variables']
type ValuesState = AppState['variables']['values']['contextID']

const getVariablesForOrgMemoized = memoizeOne(
  (variablesState: VariablesState, orgID: string) => {
    return Object.values(variablesState)
      .filter(
        d => d.status === RemoteDataState.Done && d.variable.orgID === orgID
      )
      .map(d => d.variable)
  }
)

export const getVariablesForOrg = (
  state: AppState,
  orgID: string
): Variable[] => {
  return getVariablesForOrgMemoized(state.variables.variables, orgID)
}

export const getValueSelections = (
  state: AppState,
  contextID: string
): ValueSelections => {
  const contextValues: VariableValues =
    get(state, `variables.values.${contextID}.values`) || {}

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

const getVariableAssignmentsMemoized = memoizeOne(
  (
    valuesState: ValuesState,
    variablesState: VariablesState
  ): VariableAssignment[] => {
    if (!valuesState) {
      return []
    }

    const result = []

    for (const [variableID, values] of Object.entries(valuesState.values)) {
      const variableName = get(variablesState, [variableID, 'variable', 'name'])

      if (variableName) {
        result.push(getVarAssignment(variableName, values))
      }
    }

    return result
  }
)

export const getVariableAssignments = (
  state: AppState,
  contextID: string
): VariableAssignment[] =>
  getVariableAssignmentsMemoized(
    state.variables.values[contextID],
    state.variables.variables
  )
