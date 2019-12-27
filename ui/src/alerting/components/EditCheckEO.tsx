// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import CheckEOHeader from 'src/alerting/components/CheckEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Actions
import {
  getCheckForTimeMachine,
  saveCheckFromTimeMachine,
} from 'src/alerting/actions/checks'
import {
  setActiveTimeMachine,
  setTimeMachineCheck,
  updateTimeMachineCheck,
} from 'src/timeMachine/actions'
import {executeQueries} from 'src/timeMachine/actions/queries'

// Types
import {
  Check,
  AppState,
  RemoteDataState,
  TimeMachineID,
  QueryView,
} from 'src/types'

interface DispatchProps {
  onGetCheckForTimeMachine: typeof getCheckForTimeMachine
  onUpdateTimeMachineCheck: typeof updateTimeMachineCheck
  onSetActiveTimeMachine: typeof setActiveTimeMachine
  onSetTimeMachineCheck: typeof setTimeMachineCheck
  onExecuteQueries: typeof executeQueries
  onSaveCheckFromTimeMachine: typeof saveCheckFromTimeMachine
}

interface StateProps {
  view: QueryView | null
  check: Partial<Check>
  checkStatus: RemoteDataState
  activeTimeMachineID: TimeMachineID
}

type Props = WithRouterProps & DispatchProps & StateProps

const EditCheckEditorOverlay: FunctionComponent<Props> = ({
  onExecuteQueries,
  onGetCheckForTimeMachine,
  onSaveCheckFromTimeMachine,
  onUpdateTimeMachineCheck,
  onSetTimeMachineCheck,
  activeTimeMachineID,
  checkStatus,
  router,
  params: {checkID, orgID},
  check,
  view,
}) => {
  useEffect(() => {
    onGetCheckForTimeMachine(checkID)
  }, [checkID])

  useEffect(() => {
    onExecuteQueries()
  }, [view])

  const handleUpdateName = (name: string) => {
    onUpdateTimeMachineCheck({name})
  }

  const handleClose = () => {
    router.push(`/orgs/${orgID}/alerting`)
    onSetTimeMachineCheck(RemoteDataState.NotStarted, null)
  }

  let loadingStatus = RemoteDataState.Loading

  if (checkStatus === RemoteDataState.Error) {
    loadingStatus = RemoteDataState.Error
  }
  if (
    checkStatus === RemoteDataState.Done &&
    activeTimeMachineID === 'alerting' &&
    check.id === checkID
  ) {
    loadingStatus = RemoteDataState.Done
  }

  return (
    <Overlay visible={true} className="veo-overlay">
      <div className="veo">
        <SpinnerContainer
          spinnerComponent={<TechnoSpinner />}
          loading={loadingStatus}
        >
          <CheckEOHeader
            key={check && check.name}
            name={check && check.name}
            onSetName={handleUpdateName}
            onCancel={handleClose}
            onSave={onSaveCheckFromTimeMachine}
          />
          <div className="veo-contents">
            <TimeMachine />
          </div>
        </SpinnerContainer>
      </div>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    timeMachines: {activeTimeMachineID},
  } = state

  const {
    alerting: {check, checkStatus},
  } = getActiveTimeMachine(state)

  const {view} = getActiveTimeMachine(state)

  return {check, checkStatus, activeTimeMachineID, view}
}

const mdtp: DispatchProps = {
  onSetTimeMachineCheck: setTimeMachineCheck,
  onUpdateTimeMachineCheck: updateTimeMachineCheck,
  onSetActiveTimeMachine: setActiveTimeMachine,
  onGetCheckForTimeMachine: getCheckForTimeMachine,
  onSaveCheckFromTimeMachine: saveCheckFromTimeMachine,
  onExecuteQueries: executeQueries,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(EditCheckEditorOverlay))
