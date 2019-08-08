// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button} from '@influxdata/clockface'

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
  const handleClickAlerting = () => {
    setActiveTab('alerting')
  }

  const handleClickQueries = () => {
    setActiveTab('queries')
  }

  if (activeTab === 'alerting') {
    return <Button text="Queries" onClick={handleClickQueries} />
  }

  return (
    <Button
      titleText="Add alerting to this query"
      text="Alerting"
      onClick={handleClickAlerting}
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
