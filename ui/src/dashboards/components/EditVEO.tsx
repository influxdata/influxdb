// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import TimeMachine from 'src/timeMachine/components/TimeMachine'
import VEOHeader from 'src/dashboards/components/VEOHeader'

// Actions
import {setName} from 'src/timeMachine/actions'
import {saveVEOView} from 'src/dashboards/actions/thunks'
import {getViewAndResultsForVEO} from 'src/views/actions/thunks'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState, RemoteDataState, QueryView, TimeMachineID} from 'src/types'

interface DispatchProps {
  getViewAndResultsForVEO: typeof getViewAndResultsForVEO
  onSetName: typeof setName
  onSaveView: typeof saveVEOView
}

interface StateProps {
  activeTimeMachineID: TimeMachineID
  view: QueryView | null
}

type Props = DispatchProps &
  StateProps &
  RouteComponentProps<{orgID: string; cellID: string; dashboardID: string}>

const EditViewVEO: FunctionComponent<Props> = ({
  activeTimeMachineID,
  getViewAndResultsForVEO,
  onSaveView,
  onSetName,
  match: {
    params: {orgID, cellID, dashboardID},
  },
  history,
  view,
}) => {
  useEffect(() => {
    // TODO split this up into "loadView" "setActiveTimeMachine"
    // and something to tell the component to pull from the context
    // of the dashboardID
    getViewAndResultsForVEO(dashboardID, cellID, 'veo')
  }, [])

  const handleClose = () => {
    history.push(`/orgs/${orgID}/dashboards/${dashboardID}`)
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
  getViewAndResultsForVEO: getViewAndResultsForVEO,
  onSetName: setName,
  onSaveView: saveVEOView,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(EditViewVEO))
