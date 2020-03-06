// Libraries
import React, {FC, useRef, useState} from 'react'

// Components
import MonacoEditor from 'react-monaco-editor'
import FluxBucketProvider from 'src/shared/components/FluxBucketProvider'
import GetResources from 'src/resources/components/GetResources'

// Utils
import FLUXLANGID from 'src/external/monaco.flux.syntax'
import THEME_NAME from 'src/external/monaco.flux.theme'
import loadServer, {LSPServer} from 'src/external/monaco.flux.server'
import {comments, submit} from 'src/external/monaco.flux.hotkeys'

// Types
import {OnChangeScript} from 'src/types/flux'
import {EditorType, ResourceType} from 'src/types'

import './FluxMonacoEditor.scss'

interface Props {
  script: string
  onChangeScript: OnChangeScript
  onSubmitScript?: () => void
  setEditorInstance?: (editor: EditorType) => void
}

const FluxEditorMonaco: FC<Props> = ({
  script,
  onChangeScript,
  onSubmitScript,
  setEditorInstance,
}) => {
  const lspServer = useRef<LSPServer>(null)
  const [docVersion, setdocVersion] = useState(2)
  const [docURI, setDocURI] = useState('')

  const editorDidMount = async (editor: EditorType) => {
    if (setEditorInstance) {
      setEditorInstance(editor)
    }

    const uri = editor.getModel().uri.toString()

    setDocURI(uri)

    comments(editor)
    submit(editor, () => {
      if (onSubmitScript) {
        onSubmitScript()
      }
    })

    editor.focus()

    try {
      lspServer.current = await loadServer()
      lspServer.current.didOpen(uri, script)
    } catch (e) {
      // TODO: notify user that lsp failed
    }
  }

  const onChange = (text: string) => {
    onChangeScript(text)
    try {
      lspServer.current.didChange(docURI, text)
      setdocVersion(docVersion + 1)
    } catch (e) {
      // TODO: notify user that lsp failed
    }
  }

  return (
    <div className="time-machine-editor" data-testid="flux-editor">
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
        editorDidMount={editorDidMount}
      />
    </div>
  )
}

export default FluxEditorMonaco
