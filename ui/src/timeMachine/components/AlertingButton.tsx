// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Button,
  IconFont,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {FeatureFlag} from 'src/shared/utils/featureFlag'
import {isDraftQueryAlertable} from 'src/timeMachine/utils/queryBuilder'

// Actions
import {toggleAlertingPanel} from 'src/timeMachine/actions'

// Types
import {AppState, TimeMachineTab, DashboardDraftQuery} from 'src/types'

interface DispatchProps {
  onClick: typeof toggleAlertingPanel
}

interface StateProps {
  activeTab: TimeMachineTab
  draftQueries: DashboardDraftQuery[]
}

type Props = DispatchProps & StateProps

const AlertingButton: FunctionComponent<Props> = ({
  activeTab,
  onClick,
  draftQueries,
}) => {
  const color =
    activeTab !== 'queries' ? ComponentColor.Secondary : ComponentColor.Default

  const alertingStatus = isDraftQueryAlertable(draftQueries)
    ? ComponentStatus.Default
    : ComponentStatus.Disabled

  return (
    <FeatureFlag name="alerting">
      <Button
        icon={IconFont.BellSolid}
        color={color}
        titleText="Add a Check to monitor this data"
        text="Monitoring & Alerting"
        onClick={onClick}
        status={alertingStatus}
      />
    </FeatureFlag>
  )
}

const mstp = (state: AppState): StateProps => {
  const {draftQueries, activeTab} = getActiveTimeMachine(state)
  return {activeTab, draftQueries}
}

const mdtp: DispatchProps = {
  onClick: toggleAlertingPanel,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(AlertingButton)
