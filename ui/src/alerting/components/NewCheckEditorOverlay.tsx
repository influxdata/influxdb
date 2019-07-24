// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {connect} from 'react-redux'

// Components
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/timeMachine/components/TimeMachine'

// Actions
import {
  updateCheck,
  setCurrentCheck,
  updateCurrentCheck,
} from 'src/alerting/actions/checks'

// Types
import {Check, AppState, RemoteDataState} from 'src/types'
import {DEFAULT_CHECK} from 'src/alerting/constants'

interface DispatchProps {
  updateCheck: typeof updateCheck
  setCurrentCheck: typeof setCurrentCheck
  updateCurrentCheck: typeof updateCurrentCheck
}

interface StateProps {
  check: Check
  status: RemoteDataState
}

type Props = DispatchProps & StateProps

const NewCheckOverlay: FunctionComponent<Props> = ({
  updateCurrentCheck,
  setCurrentCheck,
  status,
  check,
}) => {
  useEffect(() => {
    setCurrentCheck(RemoteDataState.Done, {...DEFAULT_CHECK, id: '9'})
  }, [])

  const handleUpdateName = (name: string) => {
    updateCurrentCheck({name})
  }

  const handleCancel = () => {}

  const handleSave = () => {}

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
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(NewCheckOverlay)
