// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'
import classnames from 'classnames'

// Components
import {DraggableResizer, Stack} from 'src/clockface'
import TimeMachineBottom from 'src/timeMachine/components/TimeMachineBottom'
import TimeMachineVis from 'src/timeMachine/components/Vis'
import TimeSeries from 'src/shared/components/TimeSeries'
import ViewOptions from 'src/timeMachine/components/view_options/ViewOptions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {AppState, DashboardQuery, TimeRange} from 'src/types/v2'

// Styles
import 'src/timeMachine/components/TimeMachine.scss'

const INITIAL_RESIZER_HANDLE = 0.5

interface StateProps {
  queries: DashboardQuery[]
  submitToken: number
  timeRange: TimeRange
  activeTab: TimeMachineTab
}

interface State {
  resizerHandlePosition: number[]
}

type Props = StateProps

class TimeMachine extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      resizerHandlePosition: [INITIAL_RESIZER_HANDLE],
    }
  }

  public render() {
    const {queries, submitToken, timeRange} = this.props
    const {resizerHandlePosition} = this.state

    return (
      <>
        <div className={this.containerClassName}>
          <TimeSeries
            queries={queries}
            submitToken={submitToken}
            implicitSubmit={false}
            variables={getTimeRangeVars(timeRange)}
          >
            {queriesState => (
              <DraggableResizer
                stackPanels={Stack.Rows}
                handlePositions={resizerHandlePosition}
                onChangePositions={this.handleResizerChange}
              >
                <DraggableResizer.Panel>
                  <div className="time-machine--top">
                    <TimeMachineVis queriesState={queriesState} />
                  </div>
                </DraggableResizer.Panel>
                <DraggableResizer.Panel>
                  <TimeMachineBottom queriesState={queriesState} />
                </DraggableResizer.Panel>
              </DraggableResizer>
            )}
          </TimeSeries>
        </div>
        {this.viewOptions}
      </>
    )
  }

  private get viewOptions(): JSX.Element {
    const {activeTab} = this.props

    if (activeTab === TimeMachineTab.Visualization) {
      return <ViewOptions />
    }
  }

  private handleResizerChange = (resizerHandlePosition: number[]): void => {
    this.setState({resizerHandlePosition})
  }

  private get containerClassName(): string {
    const {activeTab} = this.props

    return classnames('time-machine', {
      'time-machine--split': activeTab === TimeMachineTab.Visualization,
    })
  }
}

const mstp = (state: AppState) => {
  const timeMachine = getActiveTimeMachine(state)
  const {activeTab} = getActiveTimeMachine(state)
  const {timeRange} = timeMachine
  const queries = get(timeMachine, 'view.properties.queries', [])
  const submitToken = timeMachine.submitToken

  return {queries, submitToken, timeRange, activeTab}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TimeMachine)
