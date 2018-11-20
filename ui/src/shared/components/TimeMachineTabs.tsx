// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import {Radio, ButtonShape} from 'src/clockface'

// Actions
import {setActiveTab} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {AppState} from 'src/types/v2'

interface StateProps {
  activeTab: TimeMachineTab
}

interface DispatchProps {
  onSetActiveTab: typeof setActiveTab
}

interface OwnProps {}

type Props = StateProps & DispatchProps & OwnProps

const TimeMachineTabs: SFC<Props> = ({activeTab, onSetActiveTab}) => {
  return (
    <Radio shape={ButtonShape.StretchToFit}>
      <Radio.Button
        id="deceo-tab-queries"
        titleText="Queries"
        value={TimeMachineTab.Queries}
        active={activeTab === TimeMachineTab.Queries}
        onClick={onSetActiveTab}
      >
        Queries
      </Radio.Button>
      <Radio.Button
        id="deceo-tab-vis"
        titleText="Visualization"
        value={TimeMachineTab.Visualization}
        active={activeTab === TimeMachineTab.Visualization}
        onClick={onSetActiveTab}
      >
        Visualization
      </Radio.Button>
    </Radio>
  )
  return null
}

const mstp = (state: AppState) => {
  const {activeTab} = getActiveTimeMachine(state)

  return {activeTab}
}

const mdtp = {
  onSetActiveTab: setActiveTab,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineTabs)
