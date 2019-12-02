import {loadWASM} from 'onigasm' // peer dependency of 'monaco-textmate'
import {Registry} from 'monaco-textmate' // peer dependency
import {wireTmGrammars} from 'monaco-editor-textmate'

export async function addSyntax(monaco) {
  await loadWASM(require(`onigasm/lib/onigasm.wasm`))

  monaco.languages.register({id: 'toml'})

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
  grammars.set('toml', 'toml')

  await wireTmGrammars(monaco, registry, grammars)
}
