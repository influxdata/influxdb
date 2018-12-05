// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import TimeMachineControls from 'src/shared/components/TimeMachineControls'
import {DraggableResizer, Stack} from 'src/clockface'
import TimeMachineBottom from 'src/shared/components/TimeMachineBottom'
import TimeMachineVis from 'src/shared/components/TimeMachineVis'
import TimeSeries from 'src/shared/components/TimeSeries'

// Constants
const INITIAL_RESIZER_HANDLE = 0.6

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {AppState, DashboardQuery} from 'src/types/v2'

// Styles
import './TimeMachine.scss'

interface StateProps {
  queries: DashboardQuery[]
  submitToken: number
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
    const {queries, submitToken} = this.props
    const {resizerHandlePosition} = this.state

    return (
      <div className="time-machine">
        <TimeSeries
          queries={queries}
          submitToken={submitToken}
          implicitSubmit={false}
        >
          {queriesState => (
            <DraggableResizer
              stackPanels={Stack.Rows}
              handlePositions={resizerHandlePosition}
              onChangePositions={this.handleResizerChange}
            >
              <DraggableResizer.Panel id="time-machine--top" minSizePixels={0}>
                <div className="time-machine--top">
                  <TimeMachineControls queriesState={queriesState} />
                  <TimeMachineVis queriesState={queriesState} />
                </div>
              </DraggableResizer.Panel>
              <DraggableResizer.Panel
                id="time-machine--bottom"
                minSizePixels={0}
              >
                <TimeMachineBottom queryStatus={queriesState.loading} />
              </DraggableResizer.Panel>
            </DraggableResizer>
          )}
        </TimeSeries>
      </div>
    )
  }

  private handleResizerChange = resizerHandlePosition => {
    this.setState({resizerHandlePosition})
  }
}

const mstp = (state: AppState) => {
  const timeMachine = getActiveTimeMachine(state)
  const queries = get(timeMachine, 'view.properties.queries', [])
  const submitToken = timeMachine.submitToken

  return {queries, submitToken}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TimeMachine)
