// APIs
import {parse} from '@influxdata/flux-parser'

// Utils
import {getMinDurationFromAST} from 'src/shared/utils/getMinDurationFromAST'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {reportError} from 'src/shared/utils/errors'
// Constants
import {WINDOW_PERIOD} from 'src/variables/constants'

// Types
import {VariableAssignment, Package} from 'src/types/ast'

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

    const queryDuration = getMinDurationFromAST(substitutedAST)

    return Math.round(queryDuration / DESIRED_POINTS_PER_GRAPH)
  } catch (error) {
    console.error(error)
    reportError(error, {
      context: {query},
      name: 'getWindowPeriod function',
    })
    return null
  }
}
