// Libraries
import React, {PureComponent, ComponentClass} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachine from 'src/shared/components/TimeMachine'

// Actions
import {setActiveTimeMachine} from 'src/shared/actions/v2/timeMachines'

// Utils
import {DE_TIME_MACHINE_ID} from 'src/shared/constants/timeMachine'
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'

interface StateProps {}

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

interface PassedProps {
  activeTab: TimeMachineTab
}

interface State {}

type Props = StateProps & DispatchProps & PassedProps

class DataExplorer extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    props.onSetActiveTimeMachine(DE_TIME_MACHINE_ID)
  }

  public render() {
    const {activeTab} = this.props

    return (
      <div className="data-explorer">
        <div className="time-machine-page">
          <HoverTimeProvider>
            <TimeMachine activeTab={activeTab} />
          </HoverTimeProvider>
        </div>
      </div>
    )
  }
}

const mdtp: DispatchProps = {
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect(
  null,
  mdtp
)(DataExplorer) as ComponentClass<PassedProps, State>
