// Libraries
import React, {FC, useState} from 'react'
import {connect} from 'react-redux'

// Components
import FluxEditor from 'src/shared/components/FluxMonacoEditor'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import FluxFunctionsToolbar from 'src/timeMachine/components/fluxFunctionsToolbar/FluxFunctionsToolbar'
import VariableToolbar from 'src/timeMachine/components/variableToolbar/VariableToolbar'
import ToolbarTab from 'src/timeMachine/components/ToolbarTab'

// Actions
import {setActiveQueryText} from 'src/timeMachine/actions'
import {saveAndExecuteQueries} from 'src/timeMachine/actions/queries'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'
import {
  formatFunctionForInsert,
  generateImport,
} from 'src/timeMachine/utils/insertFunction'

// Constants
import {HANDLE_VERTICAL, HANDLE_NONE} from 'src/shared/constants'

// Types
import {AppState, FluxToolbarFunction, EditorType} from 'src/types'

interface StateProps {
  activeQueryText: string
  activeTab: string
}

interface DispatchProps {
  onSetActiveQueryText: typeof setActiveQueryText
  onSubmitQueries: typeof saveAndExecuteQueries
}

type Props = StateProps & DispatchProps

const TimeMachineFluxEditor: FC<Props> = ({
  activeQueryText,
  onSubmitQueries,
  onSetActiveQueryText,
  activeTab,
}) => {
  const [displayFluxFunctions, setDisplayFluxFunctions] = useState(true)
  const [editorInstance, setEditorInstance] = useState<EditorType>(null)

  const showFluxFunctions = () => {
    setDisplayFluxFunctions(true)
  }

  const hideFluxFunctions = () => {
    setDisplayFluxFunctions(false)
  }

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

  const handleInsertFluxFunction = (func: FluxToolbarFunction): void => {
    const p = editorInstance.getPosition()
    // sets the range based on the current position
    let range = new window.monaco.Range(
      p.lineNumber,
      p.column,
      p.lineNumber,
      p.column
    )
    // edge case for when user toggles to the script editor
    // this defaults the cursor to the initial position (top-left, 1:1 position)
    if (p.lineNumber === 1 && p.column === 1) {
      const [currentRange] = editorInstance.getVisibleRanges()
      // adds the function to the end of the query
      range = new window.monaco.Range(
        currentRange.endLineNumber + 1,
        p.column,
        currentRange.endLineNumber + 1,
        p.column
      )
    }
    const edits = [
      {
        range,
        text: formatFunctionForInsert(func.name, func.example),
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

  const divisions = [
    {
      size: 0.75,
      handleDisplay: HANDLE_NONE,
      render: () => {
        return (
          <FluxEditor
            script={activeQueryText}
            onChangeScript={onSetActiveQueryText}
            onSubmitScript={onSubmitQueries}
            setEditorInstance={setEditorInstance}
          />
        )
      },
    },
    {
      style: {overflow: 'hidden'},
      render: () => {
        return (
          <>
            <div className="toolbar-tab-container">
              {activeTab !== 'customCheckQuery' && (
                <ToolbarTab
                  onSetActive={hideFluxFunctions}
                  name="Variables"
                  active={!displayFluxFunctions}
                />
              )}
              <ToolbarTab
                onSetActive={showFluxFunctions}
                name="Functions"
                active={displayFluxFunctions}
                testID="functions-toolbar-tab"
              />
            </div>
            {displayFluxFunctions ? (
              <FluxFunctionsToolbar
                onInsertFluxFunction={handleInsertFluxFunction}
              />
            ) : (
              <VariableToolbar onClickVariable={handleInsertVariable} />
            )}
          </>
        )
      },
      handlePixels: 6,
      size: 0.25,
    },
  ]

  return (
    <div className="time-machine-flux-editor">
      <Threesizer orientation={HANDLE_VERTICAL} divisions={divisions} />
    </div>
  )
}

const mstp = (state: AppState) => {
  const activeQueryText = getActiveQuery(state).text
  const {activeTab} = getActiveTimeMachine(state)

  return {activeQueryText, activeTab}
}

const mdtp = {
  onSetActiveQueryText: setActiveQueryText,
  onSubmitQueries: saveAndExecuteQueries,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(TimeMachineFluxEditor)
