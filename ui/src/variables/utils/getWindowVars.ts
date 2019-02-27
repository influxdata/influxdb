// Libraries
import {get} from 'lodash'

// APIs
import {client} from 'src/utils/api'

// Utils
import {getMinDurationFromAST} from 'src/shared/utils/getMinDurationFromAST'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'

// Constants
import {WINDOW_PERIOD} from 'src/variables/constants'
import {DEFAULT_DURATION_MS} from 'src/shared/constants'

// Types
import {VariableAssignment} from 'src/types/ast'
import {Package} from 'src/types/ast'

const DESIRED_POINTS_PER_GRAPH = 360
const FALLBACK_WINDOW_PERIOD = 15000

export const getWindowVars = async (
  query: string,
  variables: VariableAssignment[]
): Promise<VariableAssignment[]> => {
  if (!query.includes(WINDOW_PERIOD)) {
    return []
  }

  const ast = await getAST(query)

  const substitutedAST = {
    ...ast,
    files: [...ast.files, buildVarsOption(variables)],
  }

  let windowPeriod: number

  try {
    windowPeriod = getWindowInterval(getMinDurationFromAST(substitutedAST))
  } catch {
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

const getAST = async (query: string): Promise<Package> => {
  try {
    const resp = await client.queries.ast(query)
    return resp
  } catch (e) {
    const message = get(e, 'response.data.error')

    if (message) {
      throw new Error(message)
    } else {
      throw e
    }
  }
}

const getWindowInterval = (
  durationMilliseconds: number = DEFAULT_DURATION_MS
) => {
  return Math.round(durationMilliseconds / DESIRED_POINTS_PER_GRAPH)
}
