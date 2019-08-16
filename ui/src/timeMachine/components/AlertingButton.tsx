// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, IconFont, ComponentColor} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Actions
import {toggleAlertingPanel} from 'src/timeMachine/actions'

// Types
import {AppState, TimeMachineTab} from 'src/types'

interface DispatchProps {
  onClick: typeof toggleAlertingPanel
}

interface StateProps {
  activeTab: TimeMachineTab
}

type Props = DispatchProps & StateProps

const AlertingButton: FunctionComponent<Props> = ({activeTab, onClick}) => {
  const color =
    activeTab !== 'queries' ? ComponentColor.Secondary : ComponentColor.Default

  return (
    <FeatureFlag name="alerting">
      <Button
        icon={IconFont.BellSolid}
        color={color}
        titleText="Add a Check to monitor this data"
        text="Monitoring & Alerting"
        onClick={onClick}
      />
    </FeatureFlag>
  )
}

const mstp = (state: AppState): StateProps => {
  const {activeTab} = getActiveTimeMachine(state)

  return {activeTab}
}

const mdtp: DispatchProps = {
  onClick: toggleAlertingPanel,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(AlertingButton)
