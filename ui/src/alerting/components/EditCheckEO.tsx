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
import {updateCheck, getCheckForTimeMachine} from 'src/alerting/actions/checks'
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
  DashboardDraftQuery,
  TimeMachineID,
  QueryView,
} from 'src/types'

interface DispatchProps {
  onUpdateCheck: typeof updateCheck
  onGetCheckForTimeMachine: typeof getCheckForTimeMachine
  onUpdateTimeMachineCheck: typeof updateTimeMachineCheck
  onSetActiveTimeMachine: typeof setActiveTimeMachine
  onSetTimeMachineCheck: typeof setTimeMachineCheck
  onExecuteQueries: typeof executeQueries
}

interface StateProps {
  view: QueryView | null
  check: Partial<Check>
  query: DashboardDraftQuery
  checkStatus: RemoteDataState
  activeTimeMachineID: TimeMachineID
}

type Props = WithRouterProps & DispatchProps & StateProps

const EditCheckEditorOverlay: FunctionComponent<Props> = ({
  onUpdateCheck,
  onExecuteQueries,
  onGetCheckForTimeMachine,
  onUpdateTimeMachineCheck,
  onSetTimeMachineCheck,
  activeTimeMachineID,
  checkStatus,
  router,
  params: {checkID, orgID},
  query,
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

  const handleSave = () => {
    // todo: update view when check has own view
    onUpdateCheck({...check, query})
    handleClose()
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
            onSave={handleSave}
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
    draftQueries,
    alerting: {check, checkStatus},
  } = getActiveTimeMachine(state)

  const {view} = getActiveTimeMachine(state)

  return {check, checkStatus, activeTimeMachineID, query: draftQueries[0], view}
}

const mdtp: DispatchProps = {
  onUpdateCheck: updateCheck,
  onSetTimeMachineCheck: setTimeMachineCheck,
  onUpdateTimeMachineCheck: updateTimeMachineCheck,
  onSetActiveTimeMachine: setActiveTimeMachine,
  onGetCheckForTimeMachine: getCheckForTimeMachine,
  onExecuteQueries: executeQueries,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(EditCheckEditorOverlay))
