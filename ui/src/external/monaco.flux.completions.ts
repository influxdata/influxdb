// Libraries
import {LSPServer} from 'src/external/monaco.flux.server'
import FLUXLANGID from 'src/external/monaco.flux.syntax'
import {
  MonacoToProtocolConverter,
  ProtocolToMonacoConverter,
} from 'monaco-languageclient/lib/monaco-converter'
import {CompletionTriggerKind} from 'monaco-languageclient/lib/services'
// Types
import {MonacoType} from 'src/types'

const m2p = new MonacoToProtocolConverter(),
  p2m = new ProtocolToMonacoConverter()

export function registerCompletion(monaco: MonacoType, server: LSPServer) {
  monaco.languages.registerSignatureHelpProvider(FLUXLANGID, {
    provideSignatureHelp: async (model, position, _token, context) => {
      const pos = m2p.asPosition(position.lineNumber, position.column)
      try {
        const help = await server.signatureHelp(
          model.uri.toString(),
          pos,
          context
        )

        return p2m.asSignatureHelpResult(help)
      } catch (e) {
        console.error(e)
      }

      return null
    },
    signatureHelpTriggerCharacters: ['(', ','],
  })

  monaco.languages.registerCompletionItemProvider(FLUXLANGID, {
    provideCompletionItems: async (model, position, context) => {
      const wordUntil = model.getWordUntilPosition(position)
      const defaultRange = new monaco.Range(
        position.lineNumber,
        wordUntil.startColumn,
        position.lineNumber,
        wordUntil.endColumn
      )
      const pos = m2p.asPosition(position.lineNumber, position.column)
      const items = await server.completionItems(model.uri.toString(), pos, {
        ...context,
        triggerKind: CompletionTriggerKind.TriggerCharacter,
      })
      return p2m.asCompletionResult(items, defaultRange)
    },
    triggerCharacters: ['.', ':'],
  })
}
