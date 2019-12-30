// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {get} from 'lodash'

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
import {resetAlertBuilder, updateName} from 'src/alerting/actions/alertBuilder'

// Types
import {AppState, RemoteDataState, TimeMachineID, QueryView} from 'src/types'

interface DispatchProps {
  onSaveCheckFromTimeMachine: typeof saveCheckFromTimeMachine
  onGetCheckForTimeMachine: typeof getCheckForTimeMachine
  onExecuteQueries: typeof executeQueries
  onResetAlertBuilder: typeof resetAlertBuilder
  onUpdateAlertBuilderName: typeof updateName
}

interface StateProps {
  view: QueryView | null
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
  }, [get(view, 'properties.queries[0]', null)])

  const handleClose = () => {
    router.push(`/orgs/${orgID}/alerting`)
    onResetAlertBuilder()
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
    alertBuilder: {checkStatus, name, id},
  } = state

  const {view} = getActiveTimeMachine(state)

  return {
    loadedCheckID: id,
    checkName: name,
    checkStatus,
    activeTimeMachineID,
    view,
  }
}

const mdtp: DispatchProps = {
  onGetCheckForTimeMachine: getCheckForTimeMachine,
  onSaveCheckFromTimeMachine: saveCheckFromTimeMachine,
  onExecuteQueries: executeQueries,
  onResetAlertBuilder: resetAlertBuilder,
  onUpdateAlertBuilderName: updateName,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(EditCheckEditorOverlay))
