// Libraries
import React, {PureComponent, ComponentClass} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineControls from 'src/shared/components/TimeMachineControls'
import ViewComponent from 'src/shared/components/cells/View'
import ViewTypeSelector from 'src/shared/components/ViewTypeSelector'
import Threesizer from 'src/shared/components/threesizer/Threesizer'

// Actions
import {setType} from 'src/shared/actions/v2/timeMachines'

// Constants
import {HANDLE_HORIZONTAL} from 'src/shared/constants'

// Types
import {AppState, View, TimeRange} from 'src/types/v2'
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface StateProps {
  view: View
  timeRange: TimeRange
}

interface PassedProps {
  activeTab: TimeMachineTab
}
interface DispatchProps {
  onUpdateType: typeof setType
}

type Props = StateProps & PassedProps & DispatchProps
class TimeMachine extends PureComponent<Props> {
  public render() {
    return (
      <div className="time-machine">
        <TimeMachineControls />

        <div className="time-machine-container">
          <Threesizer
            orientation={HANDLE_HORIZONTAL}
            divisions={this.horizontalDivisions}
          />
        </div>
      </div>
    )
  }

  private get horizontalDivisions() {
    return [
      {
        name: '',
        handleDisplay: 'none',
        headerButtons: [],
        menuOptions: [],
        render: () => this.visualization,
        headerOrientation: HANDLE_HORIZONTAL,
        size: 0.33,
      },
      {
        name: '',
        handlePixels: 8,
        headerButtons: [],
        menuOptions: [],
        render: () => this.customizationPanels,
        headerOrientation: HANDLE_HORIZONTAL,
        size: 0.67,
      },
    ]
  }

  private get visualization(): JSX.Element {
    const {view, timeRange} = this.props
    const noop = () => {}

    return (
      <div className="time-machine-top">
        <div className="time-machine-vis">
          <div className="graph-container">
            <ViewComponent
              view={view}
              onZoom={noop}
              templates={[]}
              timeRange={timeRange}
              autoRefresh={0}
              manualRefresh={0}
              onEditCell={noop}
            />
          </div>
        </div>
      </div>
    )
  }

  private get customizationPanels(): JSX.Element {
    const {view, onUpdateType, activeTab} = this.props

    return (
      <div className="time-machine-customization">
        {activeTab === TimeMachineTab.Queries ? (
          <div />
        ) : (
          <ViewTypeSelector
            type={view.properties.type}
            onUpdateType={onUpdateType}
          />
        )}
      </div>
    )
  }
}

const mstp = (state: AppState) => {
  const {
    timeMachines: {activeTimeMachineID, timeMachines},
  } = state
  const timeMachine = timeMachines[activeTimeMachineID]

  return {
    view: timeMachine.view,
    timeRange: timeMachine.timeRange,
  }
}

const mdtp = {
  onUpdateType: setType,
}

export default connect(mstp, mdtp)(TimeMachine) as ComponentClass<PassedProps>
