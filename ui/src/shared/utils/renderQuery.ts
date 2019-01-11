// Utils
import {queryAPI} from 'src/utils/api'
import {getMinDurationFromAST} from 'src/shared/utils/getMinDurationFromAST'

// Constants
import {DEFAULT_DURATION_MS, WINDOW_PERIOD} from 'src/shared/constants'

// Types
import {InfluxLanguage} from 'src/types/v2/dashboards'

const DESIRED_POINTS_PER_GRAPH = 1440
const FALLBACK_WINDOW_PERIOD = 15000

export async function renderQuery(
  query: string,
  type: InfluxLanguage,
  variables: {[name: string]: string}
): Promise<string> {
  if (type === InfluxLanguage.InfluxQL) {
    // We don't support template variables / macros in InfluxQL yet, so this is
    // a no-op
    return query
  }

  let preamble = formatVariables(variables)

  if (query.includes(WINDOW_PERIOD)) {
    const ast = await getAST(`${preamble}\n\n${query}`)

    let windowPeriod: number

    try {
      windowPeriod = getWindowInterval(getMinDurationFromAST(ast))
    } catch {
      windowPeriod = FALLBACK_WINDOW_PERIOD
    }

    preamble += `\n${WINDOW_PERIOD} = ${windowPeriod}ms`
  }

  const renderedQuery = preamble ? `${preamble}\n\n${query}` : query

  return renderedQuery
}

function formatVariables(variables: {[name: string]: string}): string {
  return Object.entries(variables)
    .map(([key, value]) => `${key} = ${value}`)
    .join('\n')
}

async function getAST(query: string): Promise<object> {
  const {data} = await queryAPI.queryAstPost(undefined, undefined, {query})

  return data.ast
}

function getWindowInterval(durationMilliseconds: number = DEFAULT_DURATION_MS) {
  return durationMilliseconds / DESIRED_POINTS_PER_GRAPH
}
