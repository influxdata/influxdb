// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ComponentStatus} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Actions
import {setType as setViewType} from 'src/timeMachine/actions'
import {setCurrentCheck} from 'src/alerting/actions/checks'
import {setActiveTab} from 'src/timeMachine/actions'

// Types
import {AppState, RemoteDataState, ViewType} from 'src/types'
import {DEFAULT_THRESHOLD_CHECK} from 'src/alerting/constants'

interface DispatchProps {
  setActiveTab: typeof setActiveTab
  setViewType: typeof setViewType
  setCurrentCheck: typeof setCurrentCheck
}

interface StateProps {
  status: ComponentStatus
  viewType: ViewType
}

type Props = DispatchProps & StateProps

const AlertingButton: FunctionComponent<Props> = ({
  setActiveTab,
  status,
  setCurrentCheck,
  viewType,
  setViewType,
}) => {
  const handleClick = () => {
    if (viewType !== 'check') {
      setCurrentCheck(RemoteDataState.Done, DEFAULT_THRESHOLD_CHECK)
      setViewType('check')
    }
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
  const {
    activeTab,
    view: {
      properties: {type: viewType},
    },
  } = getActiveTimeMachine(state)
  let status = ComponentStatus.Default
  if (activeTab === 'alerting') {
    status = ComponentStatus.Disabled
  }
  return {status, viewType}
}

const mdtp: DispatchProps = {
  setActiveTab: setActiveTab,
  setViewType: setViewType,
  setCurrentCheck: setCurrentCheck,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(AlertingButton)
