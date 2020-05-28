import {loadWASM} from 'onigasm' // peer dependency of 'monaco-textmate'
import {Registry, StackElement, INITIAL} from 'monaco-textmate' // peer dependency

let wasm: boolean = false
let loading: boolean = false
const queue: Array<() => void> = []
const grammars = new Map()
const grammarDefs = {}

const DEFAULT_DEF = async () => ({
  format: 'json',
  content: await import(/* webpackPrefetch: 0 */ 'src/external/plaintext.tmLanguage.json').then(
    data => {
      return JSON.stringify(data)
    }
  ),
})

// NOTE: this comes from the monaco-editor-textmate package
class TokenizerState {
  constructor(private _ruleStack: StackElement) {}

  public get ruleStack(): StackElement {
    return this._ruleStack
  }

  public clone(): TokenizerState {
    return new TokenizerState(this._ruleStack)
  }

  public equals(other): boolean {
    if (
      !other ||
      !(other instanceof TokenizerState) ||
      other !== this ||
      other._ruleStack !== this._ruleStack
    ) {
      return false
    }
    return true
  }
}

async function loader() {
  return new Promise(resolve => {
    if (wasm) {
      resolve()
      return
    }

    queue.push(resolve)

    if (loading) {
      return
    }

    loading = true

    loadWASM(require(`onigasm/lib/onigasm.wasm`)).then(async () => {
      wasm = true

      const registry = new Registry({
        getGrammarDefinition: async scope => {
          if (!grammarDefs.hasOwnProperty(scope)) {
            return await DEFAULT_DEF()
          }

          return await grammarDefs[scope]()
        },
      })

      Promise.all(
        Array.from(grammars.keys()).map(async lang => {
          const grammar = await registry.loadGrammar(grammars.get(lang))
          window.monaco.languages.setTokensProvider(lang, {
            getInitialState: () => new TokenizerState(INITIAL),
            tokenize: (line: string, state: TokenizerState) => {
              const res = grammar.tokenizeLine(line, state.ruleStack)

              return {
                endState: new TokenizerState(res.ruleStack),
                tokens: res.tokens.map(token => ({
                  ...token,
                  scopes: token.scopes[token.scopes.length - 1],
                })),
              }
            },
          })
        })
      ).then(() => {
        queue.forEach(c => c())
      })
    })
  })
}

export default async function register(scope, definition) {
  window.monaco.languages.register({id: scope})

  grammars.set(scope, scope)
  grammarDefs[scope] = definition

  await loader()
}
