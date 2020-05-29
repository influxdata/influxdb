// APIs
import {parse} from 'src/external/parser'

// Utils
import {getMinDurationFromAST} from 'src/shared/utils/getMinDurationFromAST'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
// Constants
import {WINDOW_PERIOD} from 'src/variables/constants'

// Types
import {VariableAssignment, Package} from 'src/types/ast'
import {RemoteDataState, Variable} from 'src/types'
import {SELECTABLE_TIME_RANGES} from 'src/shared/constants/timeRanges'

const DESIRED_POINTS_PER_GRAPH = 360
const FALLBACK_WINDOW_PERIOD = 15000

/*
  Compute the `v.windowPeriod` variable assignment for a query.
*/
export const getWindowVars = (
  query: string,
  variables: VariableAssignment[]
): VariableAssignment[] => {
  if (!query.includes(WINDOW_PERIOD)) {
    return []
  }

  const windowPeriod =
    getWindowPeriod(query, variables) || FALLBACK_WINDOW_PERIOD

  return [
    {
      type: 'VariableAssignment',
      id: {
        type: 'Identifier',
        name: WINDOW_PERIOD,
      },
      init: {
        type: 'DurationLiteral',
        values: [{magnitude: windowPeriod, unit: 'ms'}],
      },
    },
  ]
}

/*
  Compute the duration (in milliseconds) to use for the `v.windowPeriod`
  variable assignment for a query.
*/
export const getWindowPeriod = (
  query: string,
  variables: VariableAssignment[]
): number | null => {
  if (query.length === 0) {
    return null
  }
  try {
    const ast = parse(query)

    const substitutedAST: Package = {
      package: '',
      type: 'Package',
      files: [ast, buildVarsOption(variables)],
    }

    const queryDuration = getMinDurationFromAST(substitutedAST) // in ms

    const foundDuration = SELECTABLE_TIME_RANGES.find(
      tr => tr.seconds * 1000 === queryDuration
    )

    if (foundDuration) {
      return foundDuration.windowPeriod
    }

    return Math.round(queryDuration / DESIRED_POINTS_PER_GRAPH)
  } catch (error) {
    return null
  }
}

export const getWindowPeriodVariable = (
  query: string,
  variables: VariableAssignment[]
): Variable[] | null => {
  const total = getWindowPeriod(query, variables)

  if (total === null) {
    return null
  }

  const windowPeriodVariable: Variable = {
    orgID: '',
    id: WINDOW_PERIOD,
    name: WINDOW_PERIOD,
    arguments: {
      type: 'system',
      values: [total],
    },
    status: RemoteDataState.Done,
    labels: [],
  }

  return [windowPeriodVariable]
}
