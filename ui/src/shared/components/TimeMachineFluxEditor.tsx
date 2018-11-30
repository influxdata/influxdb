// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import FluxEditor from 'src/shared/components/FluxEditor'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import FluxFunctionsToolbar from 'src/shared/components/flux_functions_toolbar/FluxFunctionsToolbar'

// Actions
import {setDraftScript, submitScript} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveDraftScript} from 'src/shared/selectors/timeMachines'

// Constants
import {HANDLE_VERTICAL, HANDLE_NONE} from 'src/shared/constants'

// Types
import {AppState} from 'src/types/v2'

import 'src/shared/components/TimeMachineFluxEditor.scss'

interface StateProps {
  draftScript: string
}

interface DispatchProps {
  onSetDraftScript: typeof setDraftScript
  onSubmitScript: typeof submitScript
}

type Props = StateProps & DispatchProps

class TimeMachineFluxEditor extends PureComponent<Props> {
  public render() {
    const {draftScript, onSetDraftScript, onSubmitScript} = this.props

    const divisions = [
      {
        size: 0.75,
        handleDisplay: HANDLE_NONE,
        render: () => (
          <FluxEditor
            script={draftScript}
            status={{type: '', text: ''}}
            onChangeScript={onSetDraftScript}
            onSubmitScript={onSubmitScript}
            suggestions={[]}
          />
        ),
      },
      {
        render: () => <FluxFunctionsToolbar />,
        handlePixels: 10,
        size: 0.25,
      },
    ]

    return (
      <div className="time-machine-flux-editor">
        <Threesizer orientation={HANDLE_VERTICAL} divisions={divisions} />
      </div>
    )
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
)(TimeMachineFluxEditor)
