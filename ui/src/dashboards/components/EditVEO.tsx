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
import {setView, getViewForTimeMachine} from 'src/dashboards/actions/views'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {TimeMachineEnum} from 'src/timeMachine/constants'

// Types
import {AppState, RemoteDataState, View, QueryView} from 'src/types'
import {ViewsState} from 'src/dashboards/reducers/views'
import {executeQueries} from 'src/timeMachine/actions/queries'

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
  onSetName: typeof setName
  onSaveView: typeof saveVEOView
  setView: typeof setView
  executeQueries: typeof executeQueries
  getViewForTimeMachine: typeof getViewForTimeMachine
}

interface StateProps {
  view: View
  loadingState: RemoteDataState
  views: ViewsState
}

type Props = DispatchProps & StateProps & WithRouterProps

const NewViewVEO: FunctionComponent<Props> = ({
  onSetActiveTimeMachine,
  getViewForTimeMachine,
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
    onSetActiveTimeMachine(TimeMachineEnum.VEO, {})

    const viewInState = get(views, `${params.cellID}.view`, null) as QueryView
    if (viewInState) {
      onSetActiveTimeMachine(TimeMachineEnum.VEO, {view: viewInState})
    } else {
      getViewForTimeMachine(
        params.dashboardID,
        params.cellID,
        TimeMachineEnum.VEO
      )
    }
  }, [params.cellID])

  useEffect(() => {
    executeQueries()
  }, [get(view, 'id', null)])

  const handleClose = () => {
    const {orgID, dashboardID} = params
    router.push(`/orgs/${orgID}/dashboards/${dashboardID}`)
  }

  const handleSave = () => {
    try {
      onSaveView(params.dashboardID)
      handleClose()
    } catch (e) {}
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

const mstp = (state: AppState, {params: {cellID}}): StateProps => {
  const {activeTimeMachineID} = state.timeMachines
  const {view} = getActiveTimeMachine(state)

  const viewMatchesRoute = get(view, 'id', null) === cellID

  let loadingState = RemoteDataState.Loading

  if (activeTimeMachineID === TimeMachineEnum.VEO && viewMatchesRoute) {
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
  executeQueries: executeQueries,
  getViewForTimeMachine: getViewForTimeMachine,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(NewViewVEO))
