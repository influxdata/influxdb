// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ComponentStatus} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Actions
import {setType as setViewType, addCheck} from 'src/timeMachine/actions'
import {setCurrentCheck} from 'src/alerting/actions/checks'
import {setActiveTab} from 'src/timeMachine/actions'

// Types
import {AppState, RemoteDataState, ViewType, TimeMachineTab} from 'src/types'
import {DEFAULT_THRESHOLD_CHECK} from 'src/alerting/constants'

interface DispatchProps {
  setActiveTab: typeof setActiveTab
  setViewType: typeof setViewType
  setCurrentCheck: typeof setCurrentCheck
  addCheck: typeof addCheck
}

interface StateProps {
  activeTab: TimeMachineTab
  viewType: ViewType
}

type Props = DispatchProps & StateProps

const AlertingButton: FunctionComponent<Props> = ({
  setActiveTab,
  addCheck,
  activeTab,
  setCurrentCheck,
  viewType,
}) => {
  const handleClick = () => {
    if (viewType !== 'check') {
      setCurrentCheck(RemoteDataState.Done, DEFAULT_THRESHOLD_CHECK)
      addCheck()
    } else {
      setActiveTab('alerting')
    }
  }

  let status = ComponentStatus.Default
  if (activeTab === 'alerting') {
    status = ComponentStatus.Disabled
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
  const {
    activeTab,
    view: {
      properties: {type: viewType},
    },
  } = getActiveTimeMachine(state)

  return {activeTab, viewType}
}

const mdtp: DispatchProps = {
  setActiveTab: setActiveTab,
  setViewType: setViewType,
  setCurrentCheck: setCurrentCheck,
  addCheck: addCheck,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(AlertingButton)
