// Libraries
import React, {FC} from 'react'

// Components
import MonacoEditor from 'react-monaco-editor'
import addTomlTheme, {THEME_NAME} from 'src/external/monaco.tomlTheme'
import {addSyntax} from 'src/external/monaco.tomlSyntax'
import {OnChangeScript} from 'src/types/flux'
import './FluxMonacoEditor.scss'

interface Position {
  line: number
  ch: number
}

interface Props {
  script: string
  onChangeScript?: OnChangeScript
  onSubmitScript?: () => void
  onCursorChange?: (position: Position) => void
}

const TomlEditorMonaco: FC<Props> = props => {
  const editorWillMount = monaco => {
    addTomlTheme(monaco)
    addSyntax(monaco)
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
    <div className="time-machine-editor--embedded" data-testid="toml-editor">
      <MonacoEditor
        language="toml"
        theme={THEME_NAME}
        value={script}
        onChange={onChangeScript}
        options={{
          fontSize: 13,
          fontFamily: '"RobotoMono", monospace',
          cursorWidth: 2,
          lineNumbersMinChars: 4,
          lineDecorationsWidth: 0,
          minimap: {
            enabled: false,
            renderCharacters: false,
          },
          overviewRulerBorder: false,
          automaticLayout: true,
          readOnly: true,
        }}
        editorWillMount={editorWillMount}
        editorDidMount={editorDidMount}
      />
    </div>
  )
}

export default TomlEditorMonaco
