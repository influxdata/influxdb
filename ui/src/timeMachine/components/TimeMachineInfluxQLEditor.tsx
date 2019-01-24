// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'
import 'src/external/codemirror'

// Actions
import {setActiveQueryText, submitScript} from 'src/timeMachine/actions'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types/v2'

const OPTIONS = {
  mode: 'influxQL',
  theme: 'influxql',
  tabIndex: 1,
  readonly: false,
  lineNumbers: true,
  autoRefresh: true,
  completeSingle: false,
  lineWrapping: true,
}

const noOp = () => {}

interface StateProps {
  activeQueryText: string
}

interface DispatchProps {
  onSetActiveQueryText: typeof setActiveQueryText
  onSubmitScript: typeof submitScript
}

type Props = StateProps & DispatchProps

class TimeMachineInfluxQLEditor extends PureComponent<Props, {}> {
  public render() {
    const {activeQueryText} = this.props

    return (
      <div className="time-machine-influxql-editor">
        <ReactCodeMirror
          autoCursor={true}
          value={activeQueryText}
          options={OPTIONS}
          onTouchStart={noOp}
          onBeforeChange={this.handleChange}
          onKeyUp={this.handleKeyUp}
        />
      </div>
    )
  }

  private handleChange = (_, __, text: string) => {
    const {onSetActiveQueryText} = this.props

    onSetActiveQueryText(text)
  }

  private handleKeyUp = (__, e: KeyboardEvent) => {
    const {ctrlKey, key} = e
    const {onSubmitScript} = this.props

    if (ctrlKey && key === 'Enter' && onSubmitScript) {
      onSubmitScript()
    }
  }
}

const mstp = (state: AppState) => {
  const activeQueryText = getActiveQuery(state).text

  return {activeQueryText}
}

const mdtp = {
  onSetActiveQueryText: setActiveQueryText,
  onSubmitScript: submitScript,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(TimeMachineInfluxQLEditor)
