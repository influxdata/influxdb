// Libraries
import React, {PureComponent, ComponentClass} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachine from 'src/shared/components/TimeMachine'

// Actions
import {setActiveTimeMachineID} from 'src/shared/actions/v2/timeMachines'

// Utils
import {DE_TIME_MACHINE_ID} from 'src/shared/constants/timeMachine'
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface StateProps {}

interface DispatchProps {
  onSetActiveTimeMachineID: typeof setActiveTimeMachineID
}

interface PassedProps {
  activeTab: TimeMachineTab
}

interface State {}

type Props = StateProps & DispatchProps & PassedProps

class DataExplorer extends PureComponent<Props, State> {
  public componentDidMount() {
    const {onSetActiveTimeMachineID} = this.props

    onSetActiveTimeMachineID(DE_TIME_MACHINE_ID)
  }

  public render() {
    const {activeTab} = this.props

    return (
      <div className="data-explorer">
        <div className="time-machine-page">
          <TimeMachine activeTab={activeTab} />
        </div>
      </div>
    )
  }
}

const mdtp: DispatchProps = {
  onSetActiveTimeMachineID: setActiveTimeMachineID,
}

export default connect(null, mdtp)(DataExplorer) as ComponentClass<
  PassedProps,
  State
>
