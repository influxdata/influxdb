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

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {createView} from 'src/shared/utils/view'

// Types
import {AppState, XYViewProperties, RemoteDataState, View} from 'src/types'

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
  onSetName: typeof setName
  onSaveView: typeof saveVEOView
}

interface StateProps {
  view: View
  loadingState: RemoteDataState
}

type Props = DispatchProps & StateProps & WithRouterProps

const NewViewVEO: FunctionComponent<Props> = ({
  onSetActiveTimeMachine,
  loadingState,
  onSaveView,
  onSetName,
  params,
  router,
  view,
}) => {
  useEffect(() => {
    const view = createView<XYViewProperties>('xy')
    onSetActiveTimeMachine('veo', {view})
  }, [])

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

const mstp = (state: AppState): StateProps => {
  const {activeTimeMachineID} = state.timeMachines
  const {view} = getActiveTimeMachine(state)

  const viewIsNew = !get(view, 'id', null)

  let loadingState = RemoteDataState.Loading

  if (activeTimeMachineID === 'veo' && viewIsNew) {
    loadingState = RemoteDataState.Done
  }

  return {view, loadingState}
}

const mdtp: DispatchProps = {
  onSetName: setName,
  onSaveView: saveVEOView,
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(NewViewVEO))
