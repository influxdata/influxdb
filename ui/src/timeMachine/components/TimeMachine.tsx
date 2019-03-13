// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'
import classnames from 'classnames'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import {DraggableResizer, Stack} from 'src/clockface'
import TimeMachineBottom from 'src/timeMachine/components/TimeMachineBottom'
import TimeMachineVis from 'src/timeMachine/components/Vis'
import TimeSeries from 'src/shared/components/TimeSeries'
import ViewOptions from 'src/timeMachine/components/view_options/ViewOptions'

// Actions
import {refreshTimeMachineVariableValues} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {getVariableAssignments} from 'src/variables/selectors'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {AppState, DashboardQuery, TimeRange} from 'src/types/v2'
import {RemoteDataState} from 'src/types'
import {VariableAssignment} from 'src/types/ast'

// Styles
import 'src/timeMachine/components/TimeMachine.scss'

const INITIAL_RESIZER_HANDLE = 0.5

interface StateProps {
  queries: DashboardQuery[]
  submitToken: number
  timeRange: TimeRange
  activeTab: TimeMachineTab
  variableAssignments: VariableAssignment[]
}

interface DispatchProps {
  onRefreshVariableValues: () => Promise<void>
}

interface State {
  resizerHandlePosition: number[]
  initialLoadingStatus: RemoteDataState
}

type Props = StateProps & DispatchProps

class TimeMachine extends Component<Props, State> {
  public state: State = {
    resizerHandlePosition: [INITIAL_RESIZER_HANDLE],
    initialLoadingStatus: RemoteDataState.Loading,
  }

  public async componentDidMount() {
    try {
      await this.props.onRefreshVariableValues()
    } catch (e) {
      console.log(e)
    }

    // Even if refreshing the variable values fails, most of the `TimeMachine`
    // can continue to function. So we set the status to `Done` whether or not
    // the refresh is successful
    this.setState({initialLoadingStatus: RemoteDataState.Done})
  }

  public render() {
    const {queries, submitToken} = this.props
    const {resizerHandlePosition, initialLoadingStatus} = this.state

    return (
      <SpinnerContainer
        loading={initialLoadingStatus}
        spinnerComponent={<TechnoSpinner />}
      >
        <div className={this.containerClassName}>
          <TimeSeries
            queries={queries}
            submitToken={submitToken}
            implicitSubmit={false}
            variables={this.variableAssignments}
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
      </SpinnerContainer>
    )
  }

  private get viewOptions(): JSX.Element {
    const {activeTab} = this.props

    if (activeTab === TimeMachineTab.Visualization) {
      return <ViewOptions />
    }
  }

  private get variableAssignments(): VariableAssignment[] {
    const {variableAssignments, timeRange} = this.props

    return [...variableAssignments, ...getTimeRangeVars(timeRange)]
  }

  private get containerClassName(): string {
    const {activeTab} = this.props

    return classnames('time-machine', {
      'time-machine--split': activeTab === TimeMachineTab.Visualization,
    })
  }

  private handleResizerChange = (resizerHandlePosition: number[]): void => {
    this.setState({resizerHandlePosition})
  }
}

const mstp = (state: AppState) => {
  const timeMachine = getActiveTimeMachine(state)
  const {activeTab, timeRange, submitToken} = timeMachine
  const queries = get(timeMachine, 'view.properties.queries', [])
  const {activeTimeMachineID} = state.timeMachines
  const variableAssignments = getVariableAssignments(state, activeTimeMachineID)

  return {queries, submitToken, timeRange, activeTab, variableAssignments}
}

const mdtp = {
  onRefreshVariableValues: refreshTimeMachineVariableValues as any,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(TimeMachine)
