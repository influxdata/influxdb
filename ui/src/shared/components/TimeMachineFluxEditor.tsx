// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import FluxEditor from 'src/shared/components/FluxEditor'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import FluxFunctionsToolbar from 'src/shared/components/flux_functions_toolbar/FluxFunctionsToolbar'

// Actions
import {
  setActiveQueryText,
  submitScript,
} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveQuery} from 'src/shared/selectors/timeMachines'

// Constants
import {HANDLE_VERTICAL, HANDLE_NONE} from 'src/shared/constants'

// Types
import {AppState} from 'src/types/v2'

import 'src/shared/components/TimeMachineFluxEditor.scss'

interface StateProps {
  activeQueryText: string
}

interface DispatchProps {
  onSetActiveQueryText: typeof setActiveQueryText
  onSubmitScript: typeof submitScript
}

type Props = StateProps & DispatchProps

class TimeMachineFluxEditor extends PureComponent<Props> {
  public render() {
    const {activeQueryText, onSetActiveQueryText, onSubmitScript} = this.props

    const divisions = [
      {
        size: 0.75,
        handleDisplay: HANDLE_NONE,
        render: () => (
          <FluxEditor
            script={activeQueryText}
            status={{type: '', text: ''}}
            onChangeScript={onSetActiveQueryText}
            onSubmitScript={onSubmitScript}
            suggestions={[]}
          />
        ),
      },
      {
        render: () => <FluxFunctionsToolbar />,
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
)(TimeMachineFluxEditor)
