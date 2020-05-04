// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import QueryBuilderPanel from 'src/notebooks/components/panels/QueryBuilderPanel'
import TimeMachineAlerting from 'src/timeMachine/components/TimeMachineAlerting'
import RawDataPanel from 'src/notebooks/components/panels/RawDataPanel'
import ViewOptions from 'src/timeMachine/components/view_options/ViewOptions'
import TimeMachineCheckQuery from 'src/timeMachine/components/TimeMachineCheckQuery'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState, TimeMachineTab} from 'src/types'

interface StateProps {
  activeTab: TimeMachineTab
  isViewingVisOptions: boolean
}

const Notebook: FunctionComponent<StateProps> = ({
  activeTab,
  isViewingVisOptions,
}) => {
  let bottomContents: JSX.Element = null

  if (activeTab === 'alerting') {
    bottomContents = <TimeMachineAlerting />
  } else if (activeTab === 'queries') {
    bottomContents = <QueryBuilderPanel />
  } else if (activeTab === 'customCheckQuery') {
    bottomContents = <TimeMachineCheckQuery />
  }

  return (
    <>
      {isViewingVisOptions && <ViewOptions />}
      <div className="notebook">
        {bottomContents}
        <RawDataPanel />
      </div>
    </>
  )
}

const mstp = (state: AppState) => {
  const {activeTab, isViewingVisOptions} = getActiveTimeMachine(state)

  return {activeTab, isViewingVisOptions}
}

export default connect<StateProps>(mstp)(Notebook)
