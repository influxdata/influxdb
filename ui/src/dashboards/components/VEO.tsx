// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/shared/components/TimeMachine'

// Actions
import {
  setName,
  setActiveTimeMachineID,
} from 'src/shared/actions/v2/timeMachines'

// Constants
import {VEO_TIME_MACHINE_ID} from 'src/shared/constants/timeMachine'

// Types
import {Source, AppState} from 'src/types/v2'
import {View} from 'src/types/v2/dashboards'
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface StateProps {
  name: string
}

interface DispatchProps {
  onSetName: typeof setName
  onSetActiveTimeMachineID: typeof setActiveTimeMachineID
}

interface OwnProps {
  source: Source
  view: View
  onHide: () => void
  onSave: (v: View) => Promise<void>
}

type Props = OwnProps & StateProps & DispatchProps

interface State {
  activeTab: TimeMachineTab
}

class VEO extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      activeTab: TimeMachineTab.Queries,
    }
  }

  public componentDidMount() {
    const {onSetActiveTimeMachineID, onSetName, view} = this.props

    onSetActiveTimeMachineID(VEO_TIME_MACHINE_ID)

    // TODO: Collect other view attributes here and set the time
    // machine state in Redux with them. Make this a single action
    const name = view.name || ''

    onSetName(name)
  }

  public render() {
    const {name, onSetName, onHide} = this.props
    const {activeTab} = this.state

    return (
      <div className="veo">
        <VEOHeader
          key={name}
          name={name}
          onSetName={onSetName}
          activeTab={activeTab}
          onSetActiveTab={this.handleSetActiveTab}
          onCancel={onHide}
          onSave={this.handleSave}
        />
        <TimeMachine activeTab={activeTab} />
      </div>
    )
  }

  private handleSetActiveTab = (activeTab: TimeMachineTab): void => {
    this.setState({activeTab})
  }

  private handleSave = (): void => {
    const {view, name, onSave} = this.props

    // TODO: Collect draft view state from Redux here
    const newView = {...view, name}

    onSave(newView)
  }
}

const mstp = (state: AppState): StateProps => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const {name} = timeMachines[activeTimeMachineID].view

  return {name}
}

const mdtp: DispatchProps = {
  onSetName: setName,
  onSetActiveTimeMachineID: setActiveTimeMachineID,
}

export default connect<StateProps, DispatchProps, OwnProps>(mstp, mdtp)(VEO)
