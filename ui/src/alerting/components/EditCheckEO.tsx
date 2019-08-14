// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import CheckEOHeader from 'src/alerting/components/CheckEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Utils
import {createView} from 'src/shared/utils/view'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Actions
import {updateCheck, getCurrentCheck} from 'src/alerting/actions/checks'
import {
  setActiveTimeMachine,
  setTimeMachineCheck,
  updateTimeMachineCheck,
} from 'src/timeMachine/actions'

// Types
import {
  Check,
  AppState,
  RemoteDataState,
  DashboardDraftQuery,
  CheckViewProperties,
  TimeMachineID,
} from 'src/types'

interface DispatchProps {
  updateCheck: typeof updateCheck
  setTimeMachineCheck: typeof setTimeMachineCheck
  getCurrentCheck: typeof getCurrentCheck
  updateTimeMachineCheck: typeof updateTimeMachineCheck
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

interface StateProps {
  check: Partial<Check>
  query: DashboardDraftQuery
  checkStatus: RemoteDataState
  activeTimeMachineID: TimeMachineID
}

type Props = WithRouterProps & DispatchProps & StateProps

const EditCheckEditorOverlay: FunctionComponent<Props> = ({
  onSetActiveTimeMachine,
  activeTimeMachineID,
  getCurrentCheck,
  checkStatus,
  updateCheck,
  router,
  params: {checkID, orgID},
  query,
  check,
}) => {
  useEffect(() => {
    if (check) {
      const view = createView<CheckViewProperties>('check')
      // todo: when check has own view get view here
      onSetActiveTimeMachine('alerting', {
        view,
        activeTab: 'alerting',
        isViewingRawData: false,
      })
    } else {
      getCurrentCheck(checkID)
    }
  }, [check, checkID])

  const handleUpdateName = (name: string) => {
    updateTimeMachineCheck({name})
  }

  const handleClose = () => {
    setTimeMachineCheck(RemoteDataState.NotStarted, null)
    router.push(`/orgs/${orgID}/alerting`)
  }

  const handleSave = () => {
    // todo: update view when check has own view
    updateCheck({...check, query})
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
    checks: {
      current: {check, status: checkStatus},
    },
    timeMachines: {activeTimeMachineID},
  } = state

  const {draftQueries} = getActiveTimeMachine(state)

  return {check, checkStatus, activeTimeMachineID, query: draftQueries[0]}
}

const mdtp: DispatchProps = {
  updateCheck: updateCheck,
  setTimeMachineCheck: setTimeMachineCheck,
  updateTimeMachineCheck: updateTimeMachineCheck,
  onSetActiveTimeMachine: setActiveTimeMachine,
  getCurrentCheck: getCurrentCheck,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(EditCheckEditorOverlay))
