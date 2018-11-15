// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import FluxEditor from 'src/shared/components/FluxEditor'
import {Button, ComponentColor} from 'src/clockface'

// Actions
import {setDraftScript, submitScript} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {AppState} from 'src/types/v2'

import 'src/shared/components/TimeMachineQueryEditor.scss'

interface StateProps {
  draftScript: string
}

interface DispatchProps {
  onSetDraftScript: typeof setDraftScript
  onSubmitScript: typeof submitScript
}

interface OwnProps {}

type Props = StateProps & DispatchProps & OwnProps

class TimeMachineQueryEditor extends PureComponent<Props> {
  public render() {
    const {draftScript, onSetDraftScript, onSubmitScript} = this.props

    return (
      <div className="time-machine-query-editor">
        <div className="time-machine-query-editor--controls">
          <Button
            text="Submit"
            onClick={onSubmitScript}
            color={ComponentColor.Primary}
          />
        </div>
        <FluxEditor
          script={draftScript}
          status={{type: '', text: ''}}
          onChangeScript={onSetDraftScript}
          suggestions={[]}
        />
      </div>
    )
  }
}

const mstp = (state: AppState) => {
  const {draftScript} = getActiveTimeMachine(state)

  return {draftScript}
}

const mdtp = {
  onSetDraftScript: setDraftScript,
  onSubmitScript: submitScript,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineQueryEditor)
