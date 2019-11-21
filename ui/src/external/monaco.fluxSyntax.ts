// TODO: this is maintained in influxdata/vsflux, which is currently
// a private repo, so we can't use it yet (alex)
import textmate from './flux.tmLanguage.json'
import {loadWASM} from 'onigasm' // peer dependency of 'monaco-textmate'
import {Registry} from 'monaco-textmate' // peer dependency
import {wireTmGrammars} from 'monaco-editor-textmate'

export async function addSyntax(monaco) {
  await loadWASM(require(`onigasm/lib/onigasm.wasm`))

  monaco.languages.register({id: 'flux'})

  const registry = new Registry({
    getGrammarDefinition: () => ({
      format: 'json',
      content: textmate,
    }),
  })

  // map of monaco "language id's" to TextMate scopeNames
  const grammars = new Map()
  grammars.set('flux', 'flux')

  await wireTmGrammars(monaco, registry, grammars)
}
