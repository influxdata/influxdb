// Libraries
import React, {FC, useRef, useState} from 'react'

// Components
import MonacoEditor from 'react-monaco-editor'
import FluxBucketProvider from 'src/shared/components/FluxBucketProvider'
import GetResources from 'src/resources/components/GetResources'

// Utils
import FLUXLANGID from 'src/external/monaco.flux.syntax'
import THEME_NAME from 'src/external/monaco.flux.theme'
import loadServer from 'src/external/monaco.flux.server'
import 'src/external/monaco.flux.completions'
import {comments, submit} from 'src/external/monaco.flux.hotkeys'
import {didChange, didOpen} from 'src/external/monaco.flux.messages'

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
  const lspServer = useRef(null)
  const [docVersion, setdocVersion] = useState(2)
  const [msgID, setmsgID] = useState(3)
  const [docURI, setDocURI] = useState(4)

  const editorDidMount = (editor: EditorType) => {
    if (setEditorInstance) {
      setEditorInstance(editor)
    }

    const uri = (editor.getModel().uri as any)._formatted
    setDocURI(uri)

    comments(editor)
    submit(editor, () => {
      if (onSubmitScript) {
        onSubmitScript()
      }
    })

    editor.focus()

    loadServer().then(server => {
      lspServer.current = server

      setdocVersion(0)
      setmsgID(0)

      lspServer.current.send(didOpen(uri, script))
    })
  }

  const onChange = (text: string) => {
    onChangeScript(text)
    lspServer.current.send(didChange(docURI, text, docVersion, msgID))
    setdocVersion(docVersion + 1)
    setmsgID(msgID + 1)
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
