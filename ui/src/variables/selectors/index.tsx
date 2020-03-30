// Libraries
import {get} from 'lodash'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'
import {getRangeVariable} from 'src/variables/utils/getTimeRangeVars'
import {getTimeRange} from 'src/dashboards/selectors'
import {getWindowPeriodVariable} from 'src/variables/utils/getWindowVars'
import {
  TIME_RANGE_START,
  TIME_RANGE_STOP,
  WINDOW_PERIOD,
} from 'src/variables/constants'

// Types
import {
  RemoteDataState,
  MapArguments,
  QueryArguments,
  CSVArguments,
} from 'src/types'
import {VariableAssignment} from 'src/types/ast'
import {AppState, VariableArgumentType, Variable} from 'src/types'

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

// this function grabs all user defined variables
// and hydrates them based on their context
export const getVariables = (
  state: AppState,
  contextID?: string | null
): Variable[] => {
  const variableIDs = get(
    state,
    ['resources', 'variables', 'values', contextID, 'order'],
    get(state, ['resources', 'variables', 'allIDs'], [])
  )

  return variableIDs
    .reduce((prev, curr) => {
      prev.push(getVariable(state, contextID, curr))

      return prev
    }, [])
    .filter(v => !!v)
}

// the same as the above method, but includes system
// variables
export const getAllVariables = (
  state: AppState,
  contextID?: string | null
): Variable[] => {
  const variableIDs = get(
    state,
    ['resources', 'variables', 'values', contextID, 'order'],
    get(state, ['resources', 'variables', 'allIDs'], [])
  ).concat([TIME_RANGE_START, TIME_RANGE_STOP, WINDOW_PERIOD])

  return variableIDs
    .reduce((prev, curr) => {
      prev.push(getVariable(state, contextID, curr))

      return prev
    }, [])
    .filter(v => !!v)
}

export const getVariable = (
  state: AppState,
  contextID: string | null,
  variableID: string
): Variable => {
  const ctx = get(state, ['resources', 'variables', 'values', contextID])
  let vari = get(state, ['resources', 'variables', 'byID', variableID])

  if (ctx && ctx.values && ctx.values.hasOwnProperty(variableID)) {
    vari = {...vari, ...ctx.values[variableID]}
  }

  if (variableID === TIME_RANGE_START || variableID === TIME_RANGE_STOP) {
    const timeRange = getTimeRange(state, contextID)

    vari = getRangeVariable(variableID, timeRange)
  }

  if (variableID === WINDOW_PERIOD) {
    const {text} = getActiveQuery(state)
    const variables = getVariables(
      state,
      contextID || state.timeMachines.activeTimeMachineID
    )
    const range = getTimeRange(state, contextID)
    const timeVars = [
      getRangeVariable(TIME_RANGE_START, range),
      getRangeVariable(TIME_RANGE_STOP, range),
    ].map(v => asAssignment(v))

    const assignments = variables.reduce((acc, curr) => {
      if (!curr.name || !curr.selected) {
        return acc
      }

      return [...acc, asAssignment(curr)]
    }, timeVars)

    vari = (getWindowPeriodVariable(text, assignments) || [])[0]
  }

  if (!vari) {
    return vari
  }

  if (vari.arguments.type === 'query') {
    if (!ctx || !ctx.values || !ctx.values.hasOwnProperty(variableID)) {
      // TODO load that ish for the context
      // hydrateQueries(state, contextID, variableID)
    }
  }

  if (!vari.asAssignment) {
    vari.asAssignment = () => asAssignment(vari)
  }

  if (!vari.selected) {
    if (vari.arguments.type === 'map') {
      vari.selected = [Object.keys(vari.arguments.values)[0]]
    } else {
      vari.selected = [vari.arguments.values[0]]
    }
  }

  return vari
}

export const asAssignment = (variable: Variable): VariableAssignment => {
  const out = {
    type: 'VariableAssignment' as const,
    id: {
      type: 'Identifier' as const,
      name: variable.name,
    },
  } as VariableAssignment

  if (variable.id === WINDOW_PERIOD) {
    out.init = {
      type: 'DurationLiteral',
      values: [{magnitude: variable.arguments.values[0] || 10000, unit: 'ms'}],
    }

    return out
  }

  if (variable.id === TIME_RANGE_START || variable.id === TIME_RANGE_STOP) {
    const val = variable.arguments.values[0]

    if (!isNaN(Date.parse(val))) {
      out.init = {
        type: 'DateTimeLiteral',
        value: new Date(val).toISOString(),
      }
    } else if (val === 'now()') {
      out.init = {
        type: 'CallExpression',
        callee: {
          type: 'Identifier',
          name: 'now',
        },
      }
    } else if (val) {
      out.init = {
        type: 'UnaryExpression',
        operator: '-',
        argument: {
          type: 'DurationLiteral',
          values: val,
        },
      }
    }

    return out
  }

  if (variable.arguments.type === 'map') {
    if (!variable.selected) {
      variable.selected = [Object.keys(variable.arguments.values)[0]]
    }
    out.init = {
      type: 'StringLiteral',
      value: variable.arguments.values[variable.selected[0]],
    }
  }

  if (variable.arguments.type === 'constant') {
    if (!variable.selected) {
      variable.selected = [variable.arguments.values[0]]
    }
    out.init = {
      type: 'StringLiteral',
      value: variable.selected[0],
    }
  }

  if (variable.arguments.type === 'query') {
    out.init = {
      type: 'StringLiteral',
      value: variable.selected[0],
    }
  }

  return out
}

// TODO kill this function
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

// TODO kill this function
export const getDashboardVariablesStatus = (
  state: AppState
): RemoteDataState => {
  return get(state, 'resources.variables.status')
}
