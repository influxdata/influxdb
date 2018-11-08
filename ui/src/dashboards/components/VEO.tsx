// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import VEOHeader from 'src/dashboards/components/VEOHeader'
import TimeMachine from 'src/shared/components/TimeMachine'

// Actions
import {setName, setActiveTimeMachine} from 'src/shared/actions/v2/timeMachines'

// Constants
import {VEO_TIME_MACHINE_ID} from 'src/shared/constants/timeMachine'

// Types
import {Source, AppState} from 'src/types/v2'
import {NewView, View} from 'src/types/v2/dashboards'
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface StateProps {
  draftView: NewView
}

interface DispatchProps {
  onSetName: typeof setName
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

interface OwnProps {
  source: Source
  view: NewView | View
  onHide: () => void
  onSave: (v: NewView | View) => Promise<void>
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
    const {onSetActiveTimeMachine, view} = this.props

    onSetActiveTimeMachine(VEO_TIME_MACHINE_ID, {view})
  }

  public render() {
    const {draftView, onSetName, onHide} = this.props
    const {activeTab} = this.state

    return (
      <div className="veo">
        <VEOHeader
          key={draftView.name}
          name={draftView.name}
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
    const {view, draftView, onSave} = this.props

    onSave({...view, ...draftView})
  }
}

const mstp = (state: AppState): StateProps => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const draftView = timeMachines[activeTimeMachineID].view

  return {draftView}
}

const mdtp: DispatchProps = {
  onSetName: setName,
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(VEO)
