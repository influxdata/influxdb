// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'
import 'src/external/codemirror'

// Actions
import {setDraftScript, submitScript} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveDraftScript} from 'src/shared/selectors/timeMachines'

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
  draftScript: string
}

interface DispatchProps {
  onSetDraftScript: typeof setDraftScript
  onSubmitScript: typeof submitScript
}

type Props = StateProps & DispatchProps

class TimeMachineInfluxQLEditor extends PureComponent<Props, {}> {
  public render() {
    const {draftScript} = this.props

    return (
      <div className="time-machine-influxql-editor">
        <ReactCodeMirror
          autoCursor={true}
          value={draftScript}
          options={OPTIONS}
          onTouchStart={noOp}
          onBeforeChange={this.handleChange}
          onKeyUp={this.handleKeyUp}
        />
      </div>
    )
  }

  private handleChange = (_, __, text: string) => {
    const {onSetDraftScript} = this.props

    onSetDraftScript(text)
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
  const draftScript = getActiveDraftScript(state)

  return {draftScript}
}

const mdtp = {
  onSetDraftScript: setDraftScript,
  onSubmitScript: submitScript,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(TimeMachineInfluxQLEditor)
