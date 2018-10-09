// Libraries
import React, {PureComponent} from 'react'

// Components
import TimeMachineControls from 'src/shared/components/TimeMachineControls'

class TimeMachine extends PureComponent {
  public render() {
    return (
      <div className="time-machine">
        <TimeMachineControls />
      </div>
    )
  }
}

export default TimeMachine
