// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ComponentColor, IconFont} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {isDraftQueryAlertable} from 'src/timeMachine/utils/queryBuilder'

// Actions
import {setActiveTab} from 'src/timeMachine/actions'

// Types
import {AppState, TimeMachineTab, DashboardDraftQuery} from 'src/types'
import {ComponentStatus} from 'src/clockface'

interface DispatchProps {
  setActiveTab: typeof setActiveTab
}

interface StateProps {
  activeTab: TimeMachineTab
  draftQueries: DashboardDraftQuery[]
}

type Props = DispatchProps & StateProps

const CheckAlertingButton: FunctionComponent<Props> = ({
  setActiveTab,
  draftQueries,
  activeTab,
}) => {
  const handleClick = () => {
    if (activeTab === 'alerting') {
      setActiveTab('queries')
    } else {
      setActiveTab('alerting')
    }
  }

  const alertingStatus = isDraftQueryAlertable(draftQueries)
    ? ComponentStatus.Default
    : ComponentStatus.Disabled

  return (
    <Button
      icon={IconFont.BellSolid}
      color={
        activeTab === 'alerting'
          ? ComponentColor.Secondary
          : ComponentColor.Default
      }
      status={alertingStatus}
      titleText="Add a Check to monitor this data"
      text="Monitoring & Alerting"
      onClick={handleClick}
    />
  )
}

const mstp = (state: AppState): StateProps => {
  const {activeTab, draftQueries} = getActiveTimeMachine(state)

  return {activeTab, draftQueries}
}

const mdtp: DispatchProps = {
  setActiveTab: setActiveTab,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(CheckAlertingButton)
