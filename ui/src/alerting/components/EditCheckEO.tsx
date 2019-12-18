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
  saveCheckFromTimeMachine,
  getCheckForTimeMachine,
} from 'src/alerting/actions/checks'
import {executeQueries} from 'src/timeMachine/actions/queries'
import {notify} from 'src/shared/actions/notifications'
import {updateCheckFailed} from 'src/shared/copy/notifications'
import {resetAlertBuilder, updateName} from 'src/alerting/actions/alertBuilder'

// Types
import {
  AppState,
  RemoteDataState,
  DashboardDraftQuery,
  TimeMachineID,
  QueryView,
} from 'src/types'

interface DispatchProps {
  onSaveCheckFromTimeMachine: typeof saveCheckFromTimeMachine
  onGetCheckForTimeMachine: typeof getCheckForTimeMachine
  onExecuteQueries: typeof executeQueries
  onNotify: typeof notify
  onResetAlertBuilder: typeof resetAlertBuilder
  onUpdateAlertBuilderName: typeof updateName
}

interface StateProps {
  view: QueryView | null
  query: DashboardDraftQuery
  checkStatus: RemoteDataState
  activeTimeMachineID: TimeMachineID
  loadedCheckID: string
  checkName: string
}

type Props = WithRouterProps & DispatchProps & StateProps

const EditCheckEditorOverlay: FunctionComponent<Props> = ({
  onUpdateAlertBuilderName,
  onResetAlertBuilder,
  onSaveCheckFromTimeMachine,
  onExecuteQueries,
  onGetCheckForTimeMachine,
  onNotify,
  activeTimeMachineID,
  checkStatus,
  router,
  params: {checkID, orgID},
  checkName,
  loadedCheckID,
  view,
}) => {
  useEffect(() => {
    onGetCheckForTimeMachine(checkID)
  }, [checkID])

  useEffect(() => {
    onExecuteQueries()
  }, [view])

  const handleClose = () => {
    router.push(`/orgs/${orgID}/alerting`)
    onResetAlertBuilder()
  }

  const handleSave = () => {
    try {
      onSaveCheckFromTimeMachine()
      handleClose()
    } catch (e) {
      console.error(e)
      onNotify(updateCheckFailed(e.message))
    }
  }

  let loadingStatus = RemoteDataState.Loading

  if (checkStatus === RemoteDataState.Error) {
    loadingStatus = RemoteDataState.Error
  }
  if (
    checkStatus === RemoteDataState.Done &&
    activeTimeMachineID === 'alerting' &&
    loadedCheckID === checkID
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
            name={checkName}
            onSetName={onUpdateAlertBuilderName}
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
    alertBuilder: {checkStatus, name, id},
  } = state

  const {draftQueries} = getActiveTimeMachine(state)

  const {view} = getActiveTimeMachine(state)

  return {
    loadedCheckID: id,
    checkName: name,
    checkStatus,
    activeTimeMachineID,
    query: draftQueries[0],
    view,
  }
}

const mdtp: DispatchProps = {
  onSaveCheckFromTimeMachine: saveCheckFromTimeMachine,
  onGetCheckForTimeMachine: getCheckForTimeMachine,
  onExecuteQueries: executeQueries,
  onNotify: notify,
  onResetAlertBuilder: resetAlertBuilder,
  onUpdateAlertBuilderName: updateName,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(EditCheckEditorOverlay))
