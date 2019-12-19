import loader from 'src/external/monaco.onigasm'
import {Registry} from 'monaco-textmate' // peer dependency
import {wireTmGrammars} from 'monaco-editor-textmate'

export async function addSyntax(monaco) {
  await loader()

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
