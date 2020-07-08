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
import {loadNewVEO} from 'src/timeMachine/actions'
import {setName} from 'src/timeMachine/actions'
import {saveVEOView} from 'src/dashboards/actions/thunks'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState, RemoteDataState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps &
  RouteComponentProps<{orgID: string; dashboardID: string}>

const NewViewVEO: FunctionComponent<Props> = ({
  activeTimeMachineID,
  onSaveView,
  onSetName,
  match: {
    params: {orgID, dashboardID},
  },
  history,
  view,
}) => {
  const dispatch = useDispatch()
  useEffect(() => {
    dispatch(loadNewVEO())
  }, [dispatch, dashboardID])

  const handleClose = () => {
    history.push(`/orgs/${orgID}/dashboards/${dashboardID}`)
  }

  const handleSave = () => {
    try {
      onSaveView(dashboardID)
      handleClose()
    } catch (error) {
      console.error(error)
    }
  }

  let loadingState = RemoteDataState.Loading
  const viewIsNew = !get(view, 'id', null)
  if (activeTimeMachineID === 'veo' && viewIsNew) {
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

export default connector(withRouter(NewViewVEO))
