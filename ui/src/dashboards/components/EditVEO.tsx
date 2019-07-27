// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import TimeMachine from 'src/timeMachine/components/TimeMachine'
import VEOHeader from 'src/dashboards/components/VEOHeader'

// Actions
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {setName} from 'src/timeMachine/actions'
import {saveVEOView} from 'src/dashboards/actions'
import {setView} from 'src/dashboards/actions/views';

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {VEO_TIME_MACHINE_ID} from 'src/timeMachine/constants'
import {getView} from 'src/dashboards/apis'

// Types
import {AppState, RemoteDataState, View, QueryView} from 'src/types'
import {ViewsState} from 'src/dashboards/reducers/views';
import {executeQueries} from 'src/timeMachine/actions/queries';

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
  onSetName: typeof setName,
  onSaveView: typeof saveVEOView,
  setView: typeof setView,
  executeQueries: typeof executeQueries,
}

interface StateProps {
  view: View
  loadingState: RemoteDataState
  views: ViewsState
}

type Props = DispatchProps & StateProps & WithRouterProps

const NewViewVEO: FunctionComponent<Props> = ({
  onSetActiveTimeMachine,
  executeQueries,
  loadingState,
  onSaveView,
  onSetName,
  params,
  router,
  views,
  view,
}) => {

  useEffect(() => {
    onSetActiveTimeMachine(VEO_TIME_MACHINE_ID, {})

    const fetchView = async () => {
      view = await getView(params.dashboardID, params.cellID) as QueryView
      onSetActiveTimeMachine(VEO_TIME_MACHINE_ID, {view})
      setView(params.cellID, view, RemoteDataState.Done)
    }

    const viewInState = get(views, `${params.cellID}.view`) as QueryView

    if (viewInState){
      onSetActiveTimeMachine(VEO_TIME_MACHINE_ID, {view:viewInState})
    } else {
      fetchView();
    }

  }, [params.cellID])

  useEffect(() => {executeQueries()}, [get(view,'id',null)])

  const handleClose = () => {
    const {orgID, dashboardID} = params
    router.push(`/orgs/${orgID}/dashboards/${dashboardID}`)
  }

  const handleSave = () => {
    onSaveView(params.dashboardID)
    handleClose()
  }

  return (
    <Overlay visible={true} className="veo-overlay">
      <div className="veo">
        <SpinnerContainer
          spinnerComponent={<TechnoSpinner />}
          loading={loadingState}
        >
          <VEOHeader
            key={view && view.name}
            name={view && view.name}
            onSetName={onSetName}
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

const mstp = (state: AppState, {params:{cellID}}): StateProps => {
  const {activeTimeMachineID} = state.timeMachines
  const {view} = getActiveTimeMachine(state)

  const viewMatchesRoute = get(view,'id', null) == cellID

  let loadingState= RemoteDataState.Loading

  if (activeTimeMachineID===VEO_TIME_MACHINE_ID &&  viewMatchesRoute) {
    loadingState = RemoteDataState.Done
  }

  const {views} = state

  return {view, loadingState, views}
}

const mdtp: DispatchProps = {
  onSetName: setName,
  setView: setView,
  onSaveView: saveVEOView,
  onSetActiveTimeMachine: setActiveTimeMachine,
  executeQueries: executeQueries
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(NewViewVEO))
