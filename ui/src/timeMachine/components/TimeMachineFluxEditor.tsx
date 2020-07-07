// Libraries
import React, {FC, useState} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import FluxEditor from 'src/shared/components/FluxMonacoEditor'
import FluxToolbar from 'src/timeMachine/components/FluxToolbar'

// Actions
import {setActiveQueryText} from 'src/timeMachine/actions'
import {saveAndExecuteQueries} from 'src/timeMachine/actions/queries'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'
import {
  functionRequiresNewLine,
  generateImport,
} from 'src/timeMachine/utils/insertFunction'

// Types
import {AppState, FluxToolbarFunction, EditorType} from 'src/types'
import * as monacoEditor from 'monaco-editor/esm/vs/editor/editor.api'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const TimeMachineFluxEditor: FC<Props> = ({
  activeQueryText,
  onSubmitQueries,
  onSetActiveQueryText,
  activeTab,
}) => {
  const [editorInstance, setEditorInstance] = useState<EditorType>(null)

  const handleInsertVariable = (variableName: string): void => {
    const p = editorInstance.getPosition()
    editorInstance.executeEdits('', [
      {
        range: new window.monaco.Range(
          p.lineNumber,
          p.column,
          p.lineNumber,
          p.column
        ),
        text: `v.${variableName}`,
      },
    ])
    onSetActiveQueryText(editorInstance.getValue())
  }

  const getInsertLineNumber = (currentLineNumber: number): number => {
    const scriptLines = activeQueryText.split('\n')

    const currentLine =
      scriptLines[currentLineNumber] || scriptLines[scriptLines.length - 1]

    // Insert on the current line if its an empty line
    if (!currentLine.trim()) {
      return currentLineNumber
    }

    return currentLineNumber + 1
  }

  const defaultColumnPosition = 1 // beginning column of the row

  const getFluxTextAndRange = (
    func: FluxToolbarFunction
  ): {text: string; range: monacoEditor.Range} => {
    const p = editorInstance.getPosition()
    const insertLineNumber = getInsertLineNumber(p.lineNumber)

    let row = insertLineNumber

    const [currentRange] = editorInstance.getVisibleRanges()
    // Determines whether the new insert line is beyond the current range
    let shouldInsertOnLastLine = insertLineNumber > currentRange.endLineNumber
    // edge case for when user toggles to the script editor
    // this defaults the cursor to the initial position (top-left, 1:1 position)
    if (p.lineNumber === 1 && p.column === defaultColumnPosition) {
      // adds the function to the end of the query
      shouldInsertOnLastLine = true
      row = currentRange.endLineNumber + 1
    }

    let text = ''
    if (shouldInsertOnLastLine) {
      text = `\n  |> ${func.example}`
    } else {
      text = `  |> ${func.example}\n`
    }

    if (functionRequiresNewLine(func.name)) {
      text = `\n${func.example}\n`
    }

    const range = new window.monaco.Range(
      row,
      defaultColumnPosition,
      row,
      defaultColumnPosition
    )

    return {text, range}
  }

  const handleInsertFluxFunction = (func: FluxToolbarFunction): void => {
    const {text, range} = getFluxTextAndRange(func)

    const edits = [
      {
        range,
        text,
      },
    ]
    const importStatement = generateImport(
      func.package,
      editorInstance.getValue()
    )
    if (importStatement) {
      edits.unshift({
        range: new window.monaco.Range(1, 1, 1, 1),
        text: `${importStatement}\n`,
      })
    }
    editorInstance.executeEdits('', edits)
    onSetActiveQueryText(editorInstance.getValue())
  }

  return (
    <div className="flux-editor">
      <div className="flux-editor--left-panel">
        <FluxEditor
          script={activeQueryText}
          onChangeScript={onSetActiveQueryText}
          onSubmitScript={onSubmitQueries}
          setEditorInstance={setEditorInstance}
        />
      </div>
      <div className="flux-editor--right-panel">
        <FluxToolbar
          activeQueryBuilderTab={activeTab}
          onInsertFluxFunction={handleInsertFluxFunction}
          onInsertVariable={handleInsertVariable}
        />
      </div>
    </div>
  )
}

export {TimeMachineFluxEditor}

const mstp = (state: AppState) => {
  const activeQueryText = getActiveQuery(state).text
  const {activeTab} = getActiveTimeMachine(state)

  return {activeQueryText, activeTab}
}

const mdtp = {
  onSetActiveQueryText: setActiveQueryText,
  onSubmitQueries: saveAndExecuteQueries,
}

const connector = connect(mstp, mdtp)

export default connector(TimeMachineFluxEditor)
