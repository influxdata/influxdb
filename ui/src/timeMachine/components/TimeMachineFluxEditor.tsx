// Libraries
import React, {PureComponent} from 'react'
import Loadable from 'react-loadable'
import {connect} from 'react-redux'
import {Position} from 'codemirror'

// Components
const spinner = <div className="time-machine-editor--loading" />

const FluxEditor = Loadable({
  loader: () => import('src/shared/components/FluxEditor'),
  loading() {
    return spinner
  },
})
const FluxMonacoEditor = Loadable({
  loader: () => import('src/shared/components/FluxMonacoEditor'),
  loading() {
    return spinner
  },
})
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import FluxFunctionsToolbar from 'src/timeMachine/components/fluxFunctionsToolbar/FluxFunctionsToolbar'
import VariableToolbar from 'src/timeMachine/components/variableToolbar/VariableToolbar'
import ToolbarTab from 'src/timeMachine/components/ToolbarTab'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Actions
import {setActiveQueryText} from 'src/timeMachine/actions'
import {saveAndExecuteQueries} from 'src/timeMachine/actions/queries'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'
import {insertFluxFunction} from 'src/timeMachine/utils/insertFunction'
import {insertVariable} from 'src/timeMachine/utils/insertVariable'

// Constants
import {HANDLE_VERTICAL, HANDLE_NONE} from 'src/shared/constants'

// Types
import {AppState, FluxToolbarFunction} from 'src/types'

interface StateProps {
  activeQueryText: string
  activeTab: string
}

interface DispatchProps {
  onSetActiveQueryText: typeof setActiveQueryText
  onSubmitQueries: typeof saveAndExecuteQueries
}

interface State {
  displayFluxFunctions: boolean
}

type Props = StateProps & DispatchProps

class TimeMachineFluxEditor extends PureComponent<Props, State> {
  private cursorPosition: Position = {line: 0, ch: 0}

  public state: State = {
    displayFluxFunctions: true,
  }

  public render() {
    const {
      activeQueryText,
      onSubmitQueries,
      onSetActiveQueryText,
      activeTab,
    } = this.props

    const divisions = [
      {
        size: 0.75,
        handleDisplay: HANDLE_NONE,
        render: () => {
          return (
            <>
              <FeatureFlag name="monacoEditor">
                <FluxMonacoEditor
                  script={activeQueryText}
                  onChangeScript={onSetActiveQueryText}
                  onSubmitScript={onSubmitQueries}
                  onCursorChange={this.handleCursorPosition}
                />
              </FeatureFlag>
              <FeatureFlag name="monacoEditor" equals={false}>
                <FluxEditor
                  script={activeQueryText}
                  status={{type: '', text: ''}}
                  onChangeScript={onSetActiveQueryText}
                  onSubmitScript={onSubmitQueries}
                  suggestions={[]}
                  onCursorChange={this.handleCursorPosition}
                />
              </FeatureFlag>
            </>
          )
        },
      },
      {
        render: () => {
          return (
            <>
              <div className="toolbar-tab-container">
                {activeTab !== 'customCheckQuery' && (
                  <ToolbarTab
                    onSetActive={this.hideFluxFunctions}
                    name="Variables"
                    active={!this.state.displayFluxFunctions}
                  />
                )}
                <ToolbarTab
                  onSetActive={this.showFluxFunctions}
                  name="Functions"
                  active={this.state.displayFluxFunctions}
                  testID="functions-toolbar-tab"
                />
              </div>
              {this.rightDivision}
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

  private get rightDivision(): JSX.Element {
    const {displayFluxFunctions} = this.state

    if (displayFluxFunctions) {
      return (
        <FluxFunctionsToolbar
          onInsertFluxFunction={this.handleInsertFluxFunction}
        />
      )
    }

    return <VariableToolbar onClickVariable={this.handleInsertVariable} />
  }

  private handleCursorPosition = (position: Position): void => {
    this.cursorPosition = position
  }

  private handleInsertVariable = (variableName: string): void => {
    const {activeQueryText} = this.props
    const {line, ch} = this.cursorPosition

    const {updatedScript, cursorPosition} = insertVariable(
      line,
      ch,
      activeQueryText,
      variableName
    )

    this.props.onSetActiveQueryText(updatedScript)

    this.handleCursorPosition(cursorPosition)
  }

  private handleInsertFluxFunction = (func: FluxToolbarFunction): void => {
    const {activeQueryText, onSetActiveQueryText} = this.props
    const {line} = this.cursorPosition

    const {updatedScript, cursorPosition} = insertFluxFunction(
      line,
      activeQueryText,
      func
    )

    onSetActiveQueryText(updatedScript)
    this.handleCursorPosition(cursorPosition)
  }

  private showFluxFunctions = () => {
    this.setState({displayFluxFunctions: true})
  }

  private hideFluxFunctions = () => {
    this.setState({displayFluxFunctions: false})
  }
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
