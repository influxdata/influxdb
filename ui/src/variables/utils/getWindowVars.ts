// APIs
import {parse} from '@influxdata/flux-parser'

// Utils
import {getMinDurationFromAST} from 'src/shared/utils/getMinDurationFromAST'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'

// Constants
import {WINDOW_PERIOD} from 'src/variables/constants'
import {DEFAULT_DURATION_MS} from 'src/shared/constants'

// Types
import {VariableAssignment, Package} from 'src/types/ast'

const DESIRED_POINTS_PER_GRAPH = 360
const FALLBACK_WINDOW_PERIOD = 15000

// Compute the v.windowPeriod variable assignment for a query
export const getWindowVars = (
  query: string,
  variables: VariableAssignment[]
): VariableAssignment[] => {
  if (!query.includes(WINDOW_PERIOD)) {
    return []
  }

  const ast = parse(query)

  const substitutedAST: Package = {
    package: '',
    type: 'Package',
    files: [ast, buildVarsOption(variables)],
  }

  let windowPeriod: number

  // Use the duration of the query to compute the value of `windowPeriod`
  try {
    windowPeriod = getWindowInterval(getMinDurationFromAST(substitutedAST))
  } catch (error) {
    console.warn(error)
    windowPeriod = FALLBACK_WINDOW_PERIOD
  }

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

const getWindowInterval = (
  durationMilliseconds: number = DEFAULT_DURATION_MS
) => {
  return Math.round(durationMilliseconds / DESIRED_POINTS_PER_GRAPH)
}
