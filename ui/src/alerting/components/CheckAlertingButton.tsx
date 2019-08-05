// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ComponentStatus} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Actions
import {setActiveTab} from 'src/timeMachine/actions'

// Types
import {AppState, TimeMachineTab} from 'src/types'

interface DispatchProps {
  setActiveTab: typeof setActiveTab
}

interface StateProps {
  activeTab: TimeMachineTab
}

type Props = DispatchProps & StateProps

const CheckAlertingButton: FunctionComponent<Props> = ({
  setActiveTab,
  activeTab,
}) => {
  let buttonStatus = ComponentStatus.Default
  if (activeTab === 'alerting') {
    buttonStatus = ComponentStatus.Disabled
  }

  const handleClick = () => {
    setActiveTab('alerting')
  }

  return (
    <Button
      titleText="Add alerting to this query"
      text="Alerting"
      onClick={handleClick}
      status={buttonStatus}
    />
  )
}

const mstp = (state: AppState): StateProps => {
  const {activeTab} = getActiveTimeMachine(state)

  return {activeTab}
}

const mdtp: DispatchProps = {
  setActiveTab: setActiveTab,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(CheckAlertingButton)
