// Libraries
import React, {PureComponent} from 'react'

// Components
import TimeMachineControls from 'src/shared/components/TimeMachineControls'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import TimeMachineBottom from 'src/shared/components/TimeMachineBottom'
import TimeMachineVis from 'src/shared/components/TimeMachineVis'

// Constants
import {HANDLE_HORIZONTAL} from 'src/shared/constants'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface Props {
  activeTab: TimeMachineTab
}

class TimeMachine extends PureComponent<Props> {
  public render() {
    const {activeTab} = this.props

    const divisions = [
      {
        name: '',
        handleDisplay: 'none',
        headerButtons: [],
        menuOptions: [],
        render: () => <TimeMachineVis />,
        headerOrientation: HANDLE_HORIZONTAL,
        size: 0.33,
      },
      {
        name: '',
        handlePixels: 8,
        headerButtons: [],
        menuOptions: [],
        render: () => <TimeMachineBottom activeTab={activeTab} />,
        headerOrientation: HANDLE_HORIZONTAL,
        size: 0.67,
      },
    ]

    return (
      <div className="time-machine">
        <TimeMachineControls />
        <div className="time-machine-container">
          <Threesizer orientation={HANDLE_HORIZONTAL} divisions={divisions} />
        </div>
      </div>
    )
  }
}

export default TimeMachine
