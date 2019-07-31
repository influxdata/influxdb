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
import {AppState} from 'src/types'

interface DispatchProps {
  setActiveTab: typeof setActiveTab
}

interface StateProps {
  status: ComponentStatus
}

type Props = DispatchProps & StateProps

const AlertingButton: FunctionComponent<Props> = ({setActiveTab, status}) => {
  const handleClick = () => {
    setActiveTab('alerting')
  }

  return (
    <Button
      titleText="Add alerting to this query"
      text="Alerting"
      onClick={handleClick}
      status={status}
    />
  )
}

const mstp = (state: AppState): StateProps => {
  const {activeTab} = getActiveTimeMachine(state)
  let status = ComponentStatus.Default
  if (activeTab === 'alerting') {
    status = ComponentStatus.Disabled
  }
  return {status}
}

const mdtp: DispatchProps = {
  setActiveTab: setActiveTab,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(AlertingButton)
