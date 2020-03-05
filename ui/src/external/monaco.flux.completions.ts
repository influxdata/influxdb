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
import {IDisposable} from 'monaco-editor'

const m2p = new MonacoToProtocolConverter(),
  p2m = new ProtocolToMonacoConverter()

export function registerCompletion(
  monaco: MonacoType,
  server: LSPServer
): IDisposable {
  const completionProvider = monaco.languages.registerCompletionItemProvider(
    FLUXLANGID,
    {
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
      triggerCharacters: [
        '.',
        ':',
        'a',
        'b',
        'c',
        'd',
        'e',
        'f',
        'g',
        'h',
        'i',
        'j',
        'k',
        'l',
        'm',
        'n',
        'o',
        'p',
        'q',
        'r',
        's',
        't',
        'u',
        'v',
        'w',
        'x',
        'y',
        'z',
      ],
    }
  )

  return completionProvider
}
