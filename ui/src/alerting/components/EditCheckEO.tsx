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
import {
  updateCheck,
  setCurrentCheck,
  updateCurrentCheck,
  getCurrentCheck,
} from 'src/alerting/actions/checks'
import {setActiveTimeMachine} from 'src/timeMachine/actions'

// Types
import {
  Check,
  AppState,
  RemoteDataState,
  XYViewProperties,
  DashboardDraftQuery,
} from 'src/types'

interface DispatchProps {
  updateCheck: typeof updateCheck
  setCurrentCheck: typeof setCurrentCheck
  updateCurrentCheck: typeof updateCurrentCheck
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

interface StateProps {
  check: Partial<Check>
  query: DashboardDraftQuery
  loadingStatus: RemoteDataState
}

type Props = WithRouterProps & DispatchProps & StateProps

const EditCheckEditorOverlay: FunctionComponent<Props> = ({
  onSetActiveTimeMachine,
  loadingStatus,
  updateCheck,
  router,
  params,
  query,
  check,
}) => {
  useEffect(() => {
    getCurrentCheck(params.checkID)
    onSetActiveTimeMachine('alerting')
  }, [params.checkID])

  useEffect(() => {
    const view = createView<XYViewProperties>('xy')
    //getView here
    onSetActiveTimeMachine('alerting', {
      view,
      activeTab: 'alerting',
      isViewingRawData: false,
    })
  }, [check.id])

  const handleUpdateName = (name: string) => {
    updateCurrentCheck({name})
  }

  const handleClose = () => {
    setCurrentCheck(RemoteDataState.NotStarted, null)
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  const handleSave = () => {
    // todo: update view when check has own view
    updateCheck({...check, query})
    handleClose()
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

const mstp = (state: AppState, {params}): StateProps => {
  const {
    checks: {
      current: {check, status: checkStatus},
    },
    timeMachines: {activeTimeMachineID},
  } = state

  const {draftQueries} = getActiveTimeMachine(state)

  let loadingStatus = RemoteDataState.Loading

  if (checkStatus === RemoteDataState.Error) {
    loadingStatus = RemoteDataState.Error
  }
  if (
    checkStatus === RemoteDataState.Done &&
    activeTimeMachineID === 'alerting' &&
    check.id === params.checkID
  ) {
    loadingStatus = RemoteDataState.Done
  }

  return {check, loadingStatus, query: draftQueries[0]}
}

const mdtp: DispatchProps = {
  updateCheck: updateCheck,
  setCurrentCheck: setCurrentCheck,
  updateCurrentCheck: updateCurrentCheck,
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(EditCheckEditorOverlay))
