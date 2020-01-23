// Libraries
import memoizeOne from 'memoize-one'
import {get} from 'lodash'

// Utils
import {getVarAssignment} from 'src/variables/utils/getVarAssignment'

// Types
import {
  RemoteDataState,
  MapArguments,
  QueryArguments,
  CSVArguments,
} from 'src/types'
import {VariableAssignment} from 'src/types/ast'
import {
  AppState,
  ResourceState,
  VariableArguments,
  VariableArgumentType,
  Variable,
  VariableValues,
  VariableValuesByID,
  ValueSelections,
} from 'src/types'

type VariablesState = ResourceState['variables']['byID']
type ValuesState = ResourceState['variables']['values']['contextID']

const extractVariablesListMemoized = memoizeOne(
  (variablesState: VariablesState): Variable[] => {
    return Object.values(variablesState).filter(
      v => v.status === RemoteDataState.Done
    )
  }
)

export const extractVariablesList = (state: AppState): Variable[] => {
  return extractVariablesListMemoized(state.resources.variables.byID)
}

export const extractVariableEditorName = (state: AppState): string => {
  return state.variableEditor.name
}

export const extractVariableEditorType = (
  state: AppState
): VariableArgumentType => {
  return state.variableEditor.selected
}

export const extractVariableEditorQuery = (state: AppState): QueryArguments => {
  return (
    state.variableEditor.argsQuery || {
      type: 'query',
      values: {
        query: '',
        language: 'flux',
      },
    }
  )
}

export const extractVariableEditorMap = (state: AppState): MapArguments => {
  return (
    state.variableEditor.argsMap || {
      type: 'map',
      values: {},
    }
  )
}

export const extractVariableEditorConstant = (
  state: AppState
): CSVArguments => {
  return (
    state.variableEditor.argsConstant || {
      type: 'constant',
      values: [],
    }
  )
}

const getVariablesForDashboardMemoized = memoizeOne(
  (variables: VariablesState, variableIDs: string[]): Variable[] => {
    const variablesForDash = []

    variableIDs.forEach(variableID => {
      const variable = get(variables, `${variableID}`)

      if (variable) {
        variablesForDash.push(variable)
      }
    })

    return variablesForDash
  }
)

export const getVariablesForDashboard = (
  state: AppState,
  dashboardID: string
): Variable[] => {
  const variableIDs = get(
    state,
    `resources.variables.values.${dashboardID}.order`,
    []
  )

  return getVariablesForDashboardMemoized(
    state.resources.variables.byID,
    variableIDs
  )
}

export const getValuesForVariable = (
  state: AppState,
  variableID: string,
  contextID: string
): VariableValues => {
  return get(
    state,
    `resources.variables.values.${contextID}.values.${variableID}`
  )
}

export const getTypeForVariable = (
  state: AppState,
  variableID: string
): VariableArguments['type'] => {
  return get(state, `resources.variables.byID.${variableID}.arguments.type`, '')
}

type ArgumentValues = {[key: string]: string} | string[]

export const getArgumentValuesForVariable = (
  state: AppState,
  variableID: string
): ArgumentValues => {
  return get(
    state,
    `resources.variables.byID.${variableID}.arguments.values`,
    {}
  )
}

export const getValueSelections = (
  state: AppState,
  contextID: string
): ValueSelections => {
  const contextValues: VariableValuesByID =
    get(state, `resources.variables.values.${contextID}.values`) || {}

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

    const result: VariableAssignment[] = Object.entries(
      valuesState.values
    ).reduce((acc, [variableID, values]) => {
      const variableName = get(variablesState, [variableID, 'name'])

      if (!variableName || !values || !values.selectedValue) {
        return acc
      }

      return [...acc, getVarAssignment(variableName, values)]
    }, [])

    return result
  }
)

export const getVariableAssignments = (
  state: AppState,
  contextID: string
): VariableAssignment[] =>
  getVariableAssignmentsMemoized(
    state.resources.variables.values[contextID],
    state.resources.variables.byID
  )

export const getTimeMachineValues = (
  state: AppState,
  variableID: string
): VariableValues => {
  const activeTimeMachineID = state.timeMachines.activeTimeMachineID
  const values = get(
    state,
    `resources.variables.values.${activeTimeMachineID}.values.${variableID}`
  )

  return values
}

export const getTimeMachineValuesStatus = (
  state: AppState
): RemoteDataState => {
  const activeTimeMachineID = state.timeMachines.activeTimeMachineID
  const valuesStatus = get(
    state,
    `resources.variables.values.${activeTimeMachineID}.status`
  )

  return valuesStatus
}

export const getDashboardVariablesStatus = (
  state: AppState
): RemoteDataState => {
  return get(state, 'resources.variables.status')
}

export const getDashboardValuesStatus = (
  state: AppState,
  dashboardID: string
): RemoteDataState => {
  return get(state, `resources.variables.values.${dashboardID}.status`)
}

export const getVariable = (state: AppState, variableID: string): Variable => {
  return get(state, `resources.variables.byID.${variableID}`)
}

export const getHydratedVariables = (
  state: AppState,
  contextID: string
): Variable[] => {
  const hydratedVariableIDs: string[] = Object.keys(
    get(state, `resources.variables.values.${contextID}.values`, {})
  )

  const hydratedVariables = Object.values(
    state.resources.variables.byID
  ).filter(v => hydratedVariableIDs.includes(v.id))

  return hydratedVariables
}
