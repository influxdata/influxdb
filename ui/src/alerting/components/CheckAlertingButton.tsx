// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ComponentColor, IconFont} from '@influxdata/clockface'

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
  const handleClick = () => {
    if (activeTab === 'alerting') {
      setActiveTab('queries')
    } else {
      setActiveTab('alerting')
    }
  }

  return (
    <Button
      icon={IconFont.BellSolid}
      color={
        activeTab === 'alerting'
          ? ComponentColor.Secondary
          : ComponentColor.Default
      }
      titleText="Add a Check to monitor this data"
      text="Monitoring & Alerting"
      onClick={handleClick}
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
