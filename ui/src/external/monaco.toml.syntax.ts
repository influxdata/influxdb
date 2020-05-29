import loader from 'src/external/monaco.onigasm'
import {Registry} from 'monaco-textmate' // peer dependency
import {wireTmGrammars} from 'monaco-editor-textmate'

import {MonacoType} from 'src/types'

const TOMLLANGID = 'toml'

export async function addSyntax(monaco: MonacoType) {
  monaco.languages.register({id: TOMLLANGID})

  await loader()

  const registry = new Registry({
    getGrammarDefinition: async () => ({
      format: 'json',
      content: await import(/* webpackPrefetch: 0 */ 'src/external/toml.tmLanguage.json').then(
        data => {
          return JSON.stringify(data)
        }
      ),
    }),
  })

  // map of monaco "language id's" to TextMate scopeNames
  const grammars = new Map()
  grammars.set(TOMLLANGID, TOMLLANGID)

  await wireTmGrammars(monaco, registry, grammars)
}

addSyntax(window.monaco)

export default TOMLLANGID
