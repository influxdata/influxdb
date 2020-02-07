// Libraries
import React, {FC, useEffect, useRef} from 'react'
import {Server} from '@influxdata/flux-lsp-browser'
import {get} from 'lodash'

// Components
import MonacoEditor from 'react-monaco-editor'

// Utils
import addFluxTheme, {THEME_NAME} from 'src/external/monaco.fluxTheme'
// import {addSnippets} from 'src/external/monaco.fluxCompletions'
import {addSyntax} from 'src/external/monaco.fluxSyntax'
import {OnChangeScript} from 'src/types/flux'
import {addKeyBindings} from 'src/external/monaco.keyBindings'

// Types
import {MonacoType, EditorType} from 'src/types'

import './FluxMonacoEditor.scss'

interface Position {
  line: number
  ch: number
}

interface Props {
  script: string
  onChangeScript: OnChangeScript
  onSubmitScript?: () => void
  onCursorChange?: (position: Position) => void
}

const initialize = {
  jsonrpc: '2.0',
  id: 1,
  method: 'initialize',
  params: {},
}

const didOpen = {
  jsonrpc: '2.0',
  id: 1,
  method: 'textDocument/didOpen',
  params: {
    textDocument: {
      uri: 'editor',
      languageId: 'flux',
      version: 0,
      text: 'from.',
    },
  },
}

const didChange = (newText: string) => ({
  jsonrpc: '2.0',
  id: 2,
  method: 'textDocument/didChange',
  params: {
    textDocument: {
      uri: 'editor',
      version: 1,
    },
    contentChanges: [
      {
        text: newText,
      },
    ],
  },
})

const parseResponse = resp => {
  const message = resp.get_message()
  if (message) {
    const split = message.split('\n')
    return get(split, '2', 'unknown response')
  } else {
    const error = resp.get_error()
    const split = error.split('\n')
    return get(split, '2', 'unknown error')
  }
}

const sendMessage = (message, server) => {
  const stringifiedMessage = JSON.stringify(message)
  const size = stringifiedMessage.length

  const resp = server.process(
    `Content-Length: ${size}\r\n\r\n` + stringifiedMessage
  )

  parseResponse(resp)
  const m = resp.get_message()
  const n = m.split('\n')
  console.log('message', JSON.parse(n[2]))
  console.log('error', resp.get_error())
}

const FluxEditorMonaco: FC<Props> = ({
  script,
  onChangeScript,
  onSubmitScript,
  onCursorChange,
}) => {
  const lspServer = useRef(new Server(false))

  useEffect(() => {
    sendMessage(initialize, lspServer.current)
    sendMessage(didOpen, lspServer.current)
  }, [])

  const editorWillMount = (monaco: MonacoType) => {
    monaco.languages.register({id: 'flux'})

    addFluxTheme(monaco)
    addSyntax(monaco)
    // addSnippets(monaco)

    monaco.languages.registerCompletionItemProvider('flux', {
      provideCompletionItems: () => {
        return {
          suggestions: [
            {
              label: 'from',
              kind: monaco.languages.CompletionItemKind.Snippet,
              insertText: ['from(bucket: ${1})', '\t|>'].join('\n'),
              insertTextRules:
                monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
              documentation: 'From-Statement',
              range: {
                startLineNumber: 1,
                startColumn: 1,
                endColumn: 6,
                endLineNumber: 1,
              },
            },
          ],
        }
      },
    })
  }

  const editorDidMount = (editor: EditorType, monaco: MonacoType) => {
    addKeyBindings(editor, monaco)
    editor.onDidChangeCursorPosition(evt => {
      const {position} = evt
      const pos = {
        line: position.lineNumber - 1,
        ch: position.column,
      }

      if (onCursorChange) {
        onCursorChange(pos)
      }
    })

    editor.onKeyUp(evt => {
      const {ctrlKey, code} = evt
      if (ctrlKey && code === 'Enter') {
        if (onSubmitScript) {
          onSubmitScript()
        }
      }
    })
  }

  const onChange = (text: string) => {
    sendMessage(didChange(text), lspServer.current)
    onChangeScript(text)
  }

  return (
    <div className="time-machine-editor" data-testid="flux-editor">
      <MonacoEditor
        language="flux"
        theme={THEME_NAME}
        value={script}
        onChange={onChange}
        options={{
          fontSize: 13,
          fontFamily: '"RobotoMono", monospace',
          cursorWidth: 2,
          lineNumbersMinChars: 4,
          lineDecorationsWidth: 0,
          minimap: {
            renderCharacters: false,
          },
          overviewRulerBorder: false,
          automaticLayout: true,
        }}
        editorWillMount={editorWillMount}
        editorDidMount={editorDidMount}
      />
    </div>
  )
}

export default FluxEditorMonaco
