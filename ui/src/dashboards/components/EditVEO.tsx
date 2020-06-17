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
import {setName} from 'src/timeMachine/actions'
import {saveVEOView} from 'src/dashboards/actions/thunks'
import {getViewForTimeMachine} from 'src/views/actions/thunks'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState, RemoteDataState, QueryView, TimeMachineID} from 'src/types'

interface DispatchProps {
  onSetName: typeof setName
  onSaveView: typeof saveVEOView
  getViewForTimeMachine: typeof getViewForTimeMachine
}

interface StateProps {
  view: QueryView | null
  activeTimeMachineID: TimeMachineID
}

type Props = DispatchProps & StateProps & WithRouterProps

const EditViewVEO: FunctionComponent<Props> = ({
  getViewForTimeMachine,
  activeTimeMachineID,
  onSaveView,
  onSetName,
  params: {orgID, cellID, dashboardID},
  router,
  view,
}) => {
  const getQueryResults = () => {
    // try to get the results from the reducer
    // if the results exist & are not expired, return them
    // otherwise execute the getViewForTimeMachine
    if (false) {
      console.log('weirdness ')
    } else {
      getViewForTimeMachine(dashboardID, cellID, 'veo')
    }
  }
  useEffect(() => {
    // TODO split this up into "loadView" "setActiveTimeMachine"
    // and something to tell the component to pull from the context
    // of the dashboardID
    getQueryResults()
  }, [])

  const handleClose = () => {
    router.push(`/orgs/${orgID}/dashboards/${dashboardID}`)
  }

  const handleSave = () => {
    try {
      onSaveView(dashboardID)
      handleClose()
    } catch (e) {}
  }

  const viewMatchesRoute = get(view, 'id', null) === cellID

  let loadingState = RemoteDataState.Loading
  if (activeTimeMachineID === 'veo' && viewMatchesRoute) {
    loadingState = RemoteDataState.Done
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

const mstp = (state: AppState): StateProps => {
  const {activeTimeMachineID} = state.timeMachines

  const {view} = getActiveTimeMachine(state)

  return {view, activeTimeMachineID}
}

const mdtp: DispatchProps = {
  onSetName: setName,
  onSaveView: saveVEOView,
  getViewForTimeMachine: getViewForTimeMachine,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<StateProps & DispatchProps>(EditViewVEO))
