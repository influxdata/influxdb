// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps, useDispatch} from 'react-redux'
import {get} from 'lodash'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import CheckEOHeader from 'src/checks/components/CheckEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Actions
import {
  updateCheckFromTimeMachine,
  getCheckForTimeMachine,
} from 'src/checks/actions/thunks'
import {executeQueries} from 'src/timeMachine/actions/queries'
import {resetAlertBuilder, updateName} from 'src/alerting/actions/alertBuilder'

// Types
import {AppState, RemoteDataState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = RouteComponentProps<{orgID: string; checkID: string}> & ReduxProps

const EditCheckEditorOverlay: FunctionComponent<Props> = ({
  onUpdateAlertBuilderName,
  onResetAlertBuilder,
  onSaveCheckFromTimeMachine,
  activeTimeMachineID,
  status,
  history,
  match: {
    params: {checkID, orgID},
  },
  checkName,
  loadedCheckID,
  view,
}) => {
  const dispatch = useDispatch()
  const query = get(view, 'properties.queries[0]', null)

  useEffect(() => {
    dispatch(getCheckForTimeMachine(checkID))
  }, [dispatch, checkID])

  useEffect(() => {
    dispatch(executeQueries())
  }, [dispatch, query])

  const handleClose = () => {
    history.push(`/orgs/${orgID}/alerting`)
    onResetAlertBuilder()
  }

  let loadingStatus = RemoteDataState.Loading

  if (status === RemoteDataState.Error) {
    loadingStatus = RemoteDataState.Error
  }
  if (
    status === RemoteDataState.Done &&
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

const mstp = (state: AppState) => {
  const {
    timeMachines: {activeTimeMachineID},
    alertBuilder: {status, name, id},
  } = state

  const {view} = getActiveTimeMachine(state)

  return {
    loadedCheckID: id,
    checkName: name,
    status,
    activeTimeMachineID,
    view,
  }
}

const mdtp = {
  onSaveCheckFromTimeMachine: updateCheckFromTimeMachine,
  onResetAlertBuilder: resetAlertBuilder,
  onUpdateAlertBuilderName: updateName,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(EditCheckEditorOverlay))
