// Libraries
import React, {FC, useRef, useState} from 'react'
import {ProtocolToMonacoConverter} from 'monaco-languageclient/lib/monaco-converter'

// Components
import MonacoEditor from 'react-monaco-editor'
import FluxBucketProvider from 'src/shared/components/FluxBucketProvider'
import GetResources from 'src/resources/components/GetResources'

// Utils
import FLUXLANGID from 'src/external/monaco.flux.syntax'
import THEME_NAME from 'src/external/monaco.flux.theme'
import loadServer, {LSPServer} from 'src/external/monaco.flux.server'
import {comments, submit} from 'src/external/monaco.flux.hotkeys'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Types
import {OnChangeScript} from 'src/types/flux'
import {EditorType, ResourceType} from 'src/types'

import './FluxMonacoEditor.scss'
import {editor as monacoEditor} from 'monaco-editor'
import {Diagnostic} from 'monaco-languageclient/lib/services'

const p2m = new ProtocolToMonacoConverter()
interface Props {
  script: string
  onChangeScript: OnChangeScript
  onSubmitScript?: () => void
  setEditorInstance?: (editor: EditorType) => void
  skipFocus?: boolean
}

const FluxEditorMonaco: FC<Props> = ({
  script,
  onChangeScript,
  onSubmitScript,
  setEditorInstance,
  skipFocus,
}) => {
  const lspServer = useRef<LSPServer>(null)
  const [editorInst, seteditorInst] = useState<EditorType | null>(null)
  const [docVersion, setdocVersion] = useState(2)
  const [docURI, setDocURI] = useState('')

  const updateDiagnostics = (diagnostics: Diagnostic[]) => {
    if (editorInst) {
      const results = p2m.asDiagnostics(diagnostics)
      monacoEditor.setModelMarkers(editorInst.getModel(), 'default', results)
    }
  }

  const editorDidMount = async (editor: EditorType) => {
    if (setEditorInstance) {
      setEditorInstance(editor)
    }

    seteditorInst(editor)

    const uri = editor.getModel().uri.toString()

    setDocURI(uri)

    comments(editor)
    submit(editor, () => {
      if (onSubmitScript) {
        onSubmitScript()
      }
    })

    try {
      lspServer.current = await loadServer()
      const diagnostics = await lspServer.current.didOpen(uri, script)
      updateDiagnostics(diagnostics)

      if (isFlagEnabled('cursorAtEOF')) {
        if (!skipFocus) {
          const lines = (script || '').split('\n')
          editor.setPosition({
            lineNumber: lines.length,
            column: lines[lines.length - 1].length + 1,
          })
          editor.focus()
        }
      } else {
        editor.focus()
      }
    } catch (e) {
      // TODO: notify user that lsp failed
    }
  }

  const onChange = async (text: string) => {
    onChangeScript(text)
    try {
      const diagnostics = await lspServer.current.didChange(docURI, text)
      updateDiagnostics(diagnostics)
      setdocVersion(docVersion + 1)
    } catch (e) {
      // TODO: notify user that lsp failed
    }
  }

  return (
    <div className="flux-editor--monaco" data-testid="flux-editor">
      <GetResources resources={[ResourceType.Buckets]}>
        <FluxBucketProvider />
      </GetResources>
      <MonacoEditor
        language={FLUXLANGID}
        theme={THEME_NAME}
        value={script}
        onChange={onChange}
        options={{
          fontSize: 13,
          fontFamily: '"IBMPlexMono", monospace',
          cursorWidth: 2,
          lineNumbersMinChars: 4,
          lineDecorationsWidth: 0,
          minimap: {
            renderCharacters: false,
          },
          overviewRulerBorder: false,
          automaticLayout: true,
        }}
        editorDidMount={editorDidMount}
      />
    </div>
  )
}

export default FluxEditorMonaco
