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

export const getVariablesForDashboard = (
  state: AppState,
  dashboardID: string
): Variable[] => {
  const {
    variables: {variables, values},
  } = state

  let variablesForDash = []

  const variablesIDs = Object.keys(get(values, `${dashboardID}.values`))

  variablesIDs.forEach(variableID => {
    const variable = get(variables, `${variableID}.variable`)

    if (variable) {
      variablesForDash.push(variable)
    }
  })

  return variablesForDash
}

export const getValuesForVariable = (
  state: AppState,
  variableID: string,
  contextID: string
): string[] => {
  const {variables} = state
  return get(variables, `values.${contextID}.values.${variableID}.values`)
}

export const getSelectedValueForVariable = (
  state: AppState,
  variableID: string,
  contextID: string
): string => {
  const {variables} = state
  return get(
    variables,
    `values.${contextID}.values.${variableID}.selectedValue`
  )
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
    if (!valuesState || !valuesState.values) {
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

export const getTimeMachineValues = (
  state: AppState,
  variableID: string
): VariableValues => {
  const activeTimeMachineID = state.timeMachines.activeTimeMachineID
  const values = get(
    state,
    `variables.values.${activeTimeMachineID}.values.${variableID}`
  )

  return values
}

export const getTimeMachineValuesStatus = (
  state: AppState
): RemoteDataState => {
  const activeTimeMachineID = state.timeMachines.activeTimeMachineID
  const valuesStatus = get(
    state,
    `variables.values.${activeTimeMachineID}.status`
  )

  return valuesStatus
}

export const getVariable = (state: AppState, variableID: string): Variable => {
  return get(state, `variables.variables.${variableID}.variable`)
}

export const getHydratedVariables = (
  state: AppState,
  contextID: string
): Variable[] => {
  const hydratedVariableIDs: string[] = Object.keys(
    get(state, `variables.values.${contextID}.values`, {})
  )

  const hydratedVariables = Object.values(state.variables.variables)
    .map(d => d.variable)
    .filter(v => hydratedVariableIDs.includes(v.id))

  return hydratedVariables
}
