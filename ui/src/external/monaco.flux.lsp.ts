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
  monaco.languages.registerDocumentSymbolProvider(FLUXLANGID, {
    provideDocumentSymbols: async (model, _token) => {
      try {
        const uri = model.uri.toString()
        const symbols = await server.symbols(uri)

        return p2m.asSymbolInformations(symbols)
      } catch (e) {
        return []
      }
    },
  })

  monaco.languages.registerDocumentFormattingEditProvider(FLUXLANGID, {
    provideDocumentFormattingEdits: async (model, _context, _token) => {
      try {
        const uri = model.uri.toString()
        const edits = await server.formatting(uri)

        return p2m.asTextEdits(edits)
      } catch (e) {
        return []
      }
    },
  })

  monaco.languages.registerFoldingRangeProvider(FLUXLANGID, {
    provideFoldingRanges: async (model, _context, _token) => {
      try {
        const ranges = await server.foldingRanges(model.uri.toString())

        return p2m.asFoldingRanges(ranges)
      } catch (e) {
        return []
      }
    },
  })

  monaco.languages.registerDefinitionProvider(FLUXLANGID, {
    provideDefinition: async (model, position, _token) => {
      const uri = model.uri.toString()
      const pos = m2p.asPosition(position.lineNumber, position.column)

      try {
        const loc = await server.definition(uri, pos)

        return p2m.asLocation(loc)
      } catch (e) {
        return []
      }
    },
  })

  monaco.languages.registerReferenceProvider(FLUXLANGID, {
    provideReferences: async (model, position, context, _token) => {
      const uri = model.uri.toString()
      const pos = m2p.asPosition(position.lineNumber, position.column)

      try {
        const locs = await server.references(uri, pos, context)

        return (locs || []).map(loc => p2m.asLocation(loc))
      } catch (e) {
        return []
      }
    },
  })

  monaco.languages.registerRenameProvider(FLUXLANGID, {
    provideRenameEdits: async (model, position, newName, _token) => {
      const uri = model.uri.toString()
      const pos = m2p.asPosition(position.lineNumber, position.column)

      try {
        const edit = await server.rename(uri, pos, newName)
        return p2m.asWorkspaceEdit(edit)
      } catch (e) {
        return null
      }
    },
  })

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
        return null
      }
    },
    signatureHelpTriggerCharacters: ['('],
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
    triggerCharacters: ['.', ':', '(', ',', '"'],
  })
}
