// Libraries
import React, {SFC} from 'react'

// Components

import TimeMachineQueryEditor from 'src/shared/components/TimeMachineQueryEditor'
import ViewOptions from 'src/shared/components/view_options/ViewOptions'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface Props {
  activeTab: TimeMachineTab
}

const TimeMachineBottom: SFC<Props> = props => {
  const {activeTab} = props

  if (activeTab === TimeMachineTab.Queries) {
    return <TimeMachineQueryEditor />
  }

  if (activeTab === TimeMachineTab.Visualization) {
    return (
      <div className="time-machine-customization">
        <ViewOptions />
      </div>
    )
  }

  return null
}

export default TimeMachineBottom
