// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Utils
import {createView} from 'src/shared/utils/view'

// Actions
import {
  updateCheck,
  setCurrentCheck,
  updateCurrentCheck,
  getCurrentCheck,
} from 'src/alerting/actions/checks'
import {setActiveTimeMachine} from 'src/timeMachine/actions'

// Types
import {Check, AppState, RemoteDataState, XYView, ViewType} from 'src/types'
import {TimeMachineEnum} from 'src/timeMachine/constants'

interface DispatchProps {
  updateCheck: typeof updateCheck
  setCurrentCheck: typeof setCurrentCheck
  updateCurrentCheck: typeof updateCurrentCheck
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

interface StateProps {
  check: Partial<Check>
  status: RemoteDataState
}

type Props = WithRouterProps & DispatchProps & StateProps

const EditCheckEditorOverlay: FunctionComponent<Props> = ({
  onSetActiveTimeMachine,
  params,
  check,
  status,
}) => {
  useEffect(() => {
    getCurrentCheck(params.checkID)
    onSetActiveTimeMachine(TimeMachineEnum.Alerting)
  }, [params.checkID])

  useEffect(() => {
    // create view properties from check
    const view = createView<XYView>(ViewType.XY)
    onSetActiveTimeMachine(TimeMachineEnum.Alerting, {view})
  }, [check.id])

  const handleUpdateName = (name: string) => {
    updateCurrentCheck({name})
  }

  const handleCancel = () => {}

  const handleSave = () => {}
  // dont render time machine until active time machine is what we expect

  return (
    <Overlay visible={true} className="veo-overlay">
      <div className="veo">
        <SpinnerContainer
          spinnerComponent={<TechnoSpinner />}
          loading={status || RemoteDataState.Loading}
        >
          <VEOHeader
            key={check && check.name}
            name={check && check.name}
            onSetName={handleUpdateName}
            onCancel={handleCancel}
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
      current: {check, status},
    },
  } = state

  return {check, status}
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
