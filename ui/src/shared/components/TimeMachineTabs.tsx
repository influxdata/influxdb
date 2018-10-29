// Libraries
import React, {PureComponent} from 'react'

// Components
import {Radio, ButtonShape} from 'src/clockface'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface Props {
  activeTab: TimeMachineTab
  onSetActiveTab: (activeTab: TimeMachineTab) => void
}

class TimeMachineTabs extends PureComponent<Props> {
  public render() {
    const {activeTab, onSetActiveTab} = this.props

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
  }
}

export default TimeMachineTabs
