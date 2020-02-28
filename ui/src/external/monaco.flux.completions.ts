// Libraries
import loader from 'src/external/monaco.flux.server'
import FLUXLANGID from 'src/external/monaco.flux.syntax'
import {completion} from 'src/external/monaco.flux.messages'
import {
  MonacoToProtocolConverter,
  ProtocolToMonacoConverter,
} from 'monaco-languageclient/lib/monaco-converter'
// Types
import {MonacoType} from 'src/types'
import {IDisposable} from 'monaco-editor'

const m2p = new MonacoToProtocolConverter(),
  p2m = new ProtocolToMonacoConverter()

function registerCompletion(monaco: MonacoType, server): IDisposable {
  const completionProvider = monaco.languages.registerCompletionItemProvider(
    FLUXLANGID,
    {
      provideCompletionItems: (model, position, context) => {
        const wordUntil = model.getWordUntilPosition(position),
          defaultRange = new monaco.Range(
            position.lineNumber,
            wordUntil.startColumn,
            position.lineNumber,
            wordUntil.endColumn
          )

        return server
          .send(
            completion(
              (model.uri as any)._formatted,
              m2p.asPosition(position.lineNumber, position.column),
              context
            )
          )
          .then(response => {
            if (!response || !response.result || !response.result.items) {
              return
            }

            return p2m.asCompletionResult(
              response.result.items || [],
              defaultRange
            )
          })
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

loader().then(server => {
  registerCompletion(window.monaco, server)
})

export default loader
