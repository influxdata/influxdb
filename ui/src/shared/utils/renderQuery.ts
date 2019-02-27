// Libraries
import {get} from 'lodash'

// APIs
import {client} from 'src/utils/api'

// Utils
import {getMinDurationFromAST} from 'src/shared/utils/getMinDurationFromAST'

// Constants
import {DEFAULT_DURATION_MS, WINDOW_PERIOD} from 'src/shared/constants'

const DESIRED_POINTS_PER_GRAPH = 360
const FALLBACK_WINDOW_PERIOD = 15000

export async function renderQuery(
  query: string,
  variables: {[name: string]: string}
): Promise<string> {
  const {imports, body} = await extractImports(query)

  let variableDeclarations = formatVariables(variables, query)

  if (query.includes(WINDOW_PERIOD)) {
    const ast = await getAST(`${variableDeclarations}\n\n${query}`)

    let windowPeriod: number

    try {
      windowPeriod = getWindowInterval(getMinDurationFromAST(ast))
    } catch {
      windowPeriod = FALLBACK_WINDOW_PERIOD
    }

    variableDeclarations += `\n${WINDOW_PERIOD} = ${windowPeriod}ms`
  }

  return `${imports}\n\n${variableDeclarations}\n\n${body}`
}

async function getAST(query: string) {
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

async function extractImports(
  query: string
): Promise<{imports: string; body: string}> {
  const ast = await getAST(query)
  const {imports, body} = ast.files[0]
  const importStatements = (imports || [])
    .map(i => i.location.source)
    .join('\n')
  const bodyStatements = (body || []).map(b => b.location.source).join('\n')
  return {imports: importStatements, body: bodyStatements}
}

function formatVariables(
  variables: {[name: string]: string},
  query: string
): string {
  return Object.entries(variables)
    .filter(([key]) => query.includes(key))
    .map(([key, value]) => `${key} = ${value}`)
    .join('\n')
}

function getWindowInterval(durationMilliseconds: number = DEFAULT_DURATION_MS) {
  return Math.round(durationMilliseconds / DESIRED_POINTS_PER_GRAPH)
}
