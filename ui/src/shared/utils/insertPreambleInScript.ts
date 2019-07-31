// Libraries
import {get} from 'lodash'

// APIs
import {postQueryAst} from 'src/client'

// Types
import {Package, ImportDeclaration, Statement} from 'src/types/ast'

export const insertPreambleInScript = async (
  script: string,
  preamble: string
) => {
  if (!script.includes('import')) {
    return `${preamble}\n\n${script}`
  }

  // TODO: Replace this with `import {parse} from '@influxdata/flux-parser'`
  // when the Flux team adds location.source data to the rust implementation
  // https://github.com/influxdata/influxdb/issues/14467
  const resp = await postQueryAst({data: {query: script}})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  const ast = resp.data.ast as Package

  const imports: ImportDeclaration[] = get(ast, 'files.0.imports', [])
  const body: Statement[] = get(ast, 'files.0.body', [])

  const importsText = imports.map(d => d.location.source).join('\n')
  const bodyText = body.map(d => d.location.source).join('\n\n')

  const result = `${importsText}\n${preamble}\n\n${bodyText}`

  return result
}
