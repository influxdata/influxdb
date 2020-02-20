import loader from 'src/external/monaco.onigasm'
import {Registry} from 'monaco-textmate' // peer dependency
import {wireTmGrammars} from 'monaco-editor-textmate'

// Constants
import {FLUXLANGID} from 'src/external/constants'

// Types
import {MonacoType} from 'src/types'

export async function addSyntax(monaco: MonacoType) {
  await loader()

  const registry = new Registry({
    // TODO: this is maintained in influxdata/vsflux, which is currently
    // a private repo, so we can't use it yet (alex)
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
