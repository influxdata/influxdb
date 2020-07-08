// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps, useDispatch} from 'react-redux'
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
import {AppState, RemoteDataState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps &
  RouteComponentProps<{orgID: string; cellID: string; dashboardID: string}>

const EditViewVEO: FunctionComponent<Props> = ({
  activeTimeMachineID,
  onSaveView,
  onSetName,
  match: {
    params: {orgID, cellID, dashboardID},
  },
  history,
  view,
}) => {
  const dispatch = useDispatch()
  useEffect(() => {
    // TODO split this up into "loadView" "setActiveTimeMachine"
    // and something to tell the component to pull from the context
    // of the dashboardID
    dispatch(getViewAndResultsForVEO(dashboardID, cellID, 'veo'))
  }, [dispatch, dashboardID, cellID])

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

const mstp = (state: AppState) => {
  const {activeTimeMachineID} = state.timeMachines
  const {view} = getActiveTimeMachine(state)

  return {view, activeTimeMachineID}
}

const mdtp = {
  onSetName: setName,
  onSaveView: saveVEOView,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(EditViewVEO))
