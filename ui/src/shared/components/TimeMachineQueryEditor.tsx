// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import FluxEditor from 'src/shared/components/FluxEditor'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from 'src/clockface'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import FluxFunctionsToolbar from 'src/shared/components/flux_functions_toolbar/FluxFunctionsToolbar'

// Actions
import {setDraftScript, submitScript} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Constants
import {HANDLE_VERTICAL} from 'src/shared/constants'

// Types
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'

import 'src/shared/components/TimeMachineQueryEditor.scss'

interface StateProps {
  draftScript: string
}

interface DispatchProps {
  onSetDraftScript: typeof setDraftScript
  onSubmitScript: typeof submitScript
}

interface OwnProps {
  queryStatus: RemoteDataState
}

type Props = StateProps & DispatchProps & OwnProps

class TimeMachineQueryEditor extends PureComponent<Props> {
  public render() {
    const {
      queryStatus,
      draftScript,
      onSetDraftScript,
      onSubmitScript,
    } = this.props

    const buttonStatus =
      queryStatus === RemoteDataState.Loading
        ? ComponentStatus.Loading
        : ComponentStatus.Default

    const divisions = [
      {
        name: 'Script',
        size: 0.75,
        headerButtons: [
          <Button
            key="foo"
            text="Submit"
            size={ComponentSize.ExtraSmall}
            status={buttonStatus}
            onClick={onSubmitScript}
            color={ComponentColor.Primary}
          />,
        ],
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
        name: 'Flux Functions',
        render: () => <FluxFunctionsToolbar />,
        size: 0.25,
      },
    ]

    return (
      <div className="time-machine-query-editor">
        <Threesizer orientation={HANDLE_VERTICAL} divisions={divisions} />
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
