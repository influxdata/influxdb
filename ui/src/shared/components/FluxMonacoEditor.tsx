// Libraries
import React, {FC} from 'react'

// Components
import MonacoEditor from 'react-monaco-editor'
import {tokenizeFlux} from 'src/external/monaco.fluxLang'
import {addFluxTheme} from 'src/external/monaco.fluxTheme'
import {addSnippets} from 'src/external/monaco.fluxCompletions'
import {OnChangeScript} from 'src/types/flux'

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

const FluxEditorMonaco: FC<Props> = props => {
  const editorWillMount = monaco => {
    tokenizeFlux(monaco)
    addFluxTheme(monaco)
    addSnippets(monaco)
  }
  const editorDidMount = editor => {
    editor.onDidChangeCursorPosition(evt => {
      const {position} = evt
      const {onCursorChange} = props
      const pos = {
        line: position.lineNumber,
        ch: position.column,
      }

      if (onCursorChange) {
        onCursorChange(pos)
      }
    })

    editor.onKeyUp(evt => {
      const {ctrlKey, code} = evt
      const {onSubmitScript} = props
      if (ctrlKey && code === 'Enter') {
        if (onSubmitScript) {
          onSubmitScript()
        }
      }
    })
  }
  const {script, onChangeScript} = props

  return (
    <div className="time-machine-editor" data-testid="flux-editor">
      <MonacoEditor
        width="800"
        height="600"
        language="flux"
        theme="vs-light"
        value={script}
        onChange={onChangeScript}
        editorWillMount={editorWillMount}
        editorDidMount={editorDidMount}
      />
    </div>
  )
}

export default FluxEditorMonaco
