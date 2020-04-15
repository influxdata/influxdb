import loader from 'src/external/monaco.onigasm'
import {Registry} from 'monaco-textmate' // peer dependency
import {wireTmGrammars} from 'monaco-editor-textmate'

import {MonacoType} from 'src/types'

const FLUXLANGID = 'flux'

async function addSyntax(monaco: MonacoType) {
  monaco.languages.register({id: FLUXLANGID})

  await loader()

  const registry = new Registry({
    getGrammarDefinition: async () => ({
      format: 'json',
      content: await import(/* webpackPrefetch: 0 */ 'src/external/flux.tmLanguage.json').then(
        data => {
          return JSON.stringify(data)
        }
      ),
    }),
  })

  // map of monaco "language id's" to TextMate scopeNames
  const grammars = new Map()
  grammars.set(FLUXLANGID, FLUXLANGID)

  monaco.languages.setLanguageConfiguration(FLUXLANGID, {
    autoClosingPairs: [
      {open: '"', close: '"'},
      {open: '[', close: ']'},
      {open: "'", close: "'"},
      {open: '{', close: '}'},
      {open: '(', close: ')'},
    ],
  })

  await wireTmGrammars(monaco, registry, grammars)
}

addSyntax(window.monaco)

export default FLUXLANGID
