// Libraries
import {completion, sendMessage} from 'src/external/monaco.lspMessages'
import {
  MonacoToProtocolConverter,
  ProtocolToMonacoConverter,
} from 'monaco-languageclient/lib/monaco-converter'
import {get} from 'lodash'

// Constants
import {FLUXLANGID} from 'src/external/constants'

// Types
import {CompletionItem} from 'monaco-languageclient/lib/services'
import {MonacoType} from 'src/types'
import {IDisposable} from 'monaco-editor'

const m2p = new MonacoToProtocolConverter()
const p2m = new ProtocolToMonacoConverter()

export const registerCompletion = (monaco: MonacoType, server): IDisposable => {
  const completionProvider = monaco.languages.registerCompletionItemProvider(
    FLUXLANGID,
    {
      provideCompletionItems: (model, position, context) => {
        const wordUntil = model.getWordUntilPosition(position)
        const defaultRange = new monaco.Range(
          position.lineNumber,
          wordUntil.startColumn,
          position.lineNumber,
          wordUntil.endColumn
        )
        const response = sendMessage(
          completion(
            m2p.asPosition(position.lineNumber, position.column),
            context
          ),
          server
        )

        const completionItems = get(
          response,
          'result.items',
          null
        ) as CompletionItem[]

        if (!completionItems) {
          return
        }
        return p2m.asCompletionResult(completionItems, defaultRange)
      },
      triggerCharacters: [
        '.',
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
