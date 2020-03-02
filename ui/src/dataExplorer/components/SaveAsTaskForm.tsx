// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import TaskForm from 'src/tasks/components/TaskForm'

// Actions
import {saveNewScript} from 'src/tasks/actions/thunks'
import {
  setTaskOption,
  clearTask,
  setNewScript,
} from 'src/tasks/actions/creators'
import {refreshTimeMachineVariableValues} from 'src/timeMachine/actions/queries'

// Utils
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {formatVarsOption} from 'src/variables/utils/formatVarsOption'
import {
  taskOptionsToFluxScript,
  addDestinationToFluxScript,
} from 'src/utils/taskOptionsToFluxScript'
import {getVariableAssignments} from 'src/variables/selectors'
import {getOrg} from 'src/organizations/selectors'
import {getActiveTimeMachine, getActiveQuery} from 'src/timeMachine/selectors'

// Types
import {
  AppState,
  TimeRange,
  VariableAssignment,
  TaskSchedule,
  TaskOptions,
  TaskOptionKeys,
  DashboardDraftQuery,
} from 'src/types'

interface OwnProps {
  dismiss: () => void
}

interface DispatchProps {
  saveNewScript: typeof saveNewScript
  setTaskOption: typeof setTaskOption
  clearTask: typeof clearTask
  setNewScript: typeof setNewScript
  refreshTimeMachineVariableValues: typeof refreshTimeMachineVariableValues
}

interface StateProps {
  taskOptions: TaskOptions
  activeQuery: DashboardDraftQuery
  newScript: string
  timeRange: TimeRange
  userDefinedVars: VariableAssignment[]
}

type Props = StateProps & OwnProps & DispatchProps

class SaveAsTaskForm extends PureComponent<Props & WithRouterProps> {
  public componentDidMount() {
    const {
      setTaskOption,
      setNewScript,
      refreshTimeMachineVariableValues,
    } = this.props

    setTaskOption({
      key: 'taskScheduleType',
      value: TaskSchedule.interval,
    })
    refreshTimeMachineVariableValues()

    setNewScript(this.activeScript)
  }

  public componentWillUnmount() {
    const {clearTask} = this.props

    clearTask()
  }

  public render() {
    const {taskOptions, dismiss} = this.props

    return (
      <TaskForm
        taskOptions={taskOptions}
        onChangeScheduleType={this.handleChangeScheduleType}
        onChangeInput={this.handleChangeInput}
        onChangeToBucketName={this.handleChangeToBucketName}
        isInOverlay={true}
        onSubmit={this.handleSubmit}
        canSubmit={this.isFormValid}
        dismiss={dismiss}
      />
    )
  }

  private get isFormValid(): boolean {
    const {
      taskOptions: {name, cron, interval},
    } = this.props

    const hasSchedule = !!cron || !!interval

    return hasSchedule && !!name && !!this.activeScript
  }

  private get activeScript(): string {
    const {activeQuery} = this.props

    return activeQuery.text
  }

  private handleSubmit = () => {
    const {saveNewScript, newScript, taskOptions, timeRange} = this.props

    // When a task runs, it does not have access to variables that we typically
    // inject into the script via the front end. So any variables that are used
    // in the script need to be embedded in the script text itself before
    // saving it as a task

    const timeRangeVars = getTimeRangeVars(timeRange)
    const windowPeriodVars = getWindowVars(newScript, timeRangeVars)

    // Don't embed variables that are not used in the script
    const vars = [
      ...timeRangeVars,
      ...windowPeriodVars,
      ...this.props.userDefinedVars,
    ].filter(assignment => newScript.includes(assignment.id.name))

    const varOption: string = formatVarsOption(vars) // option v = { ... }
    const taskOption: string = taskOptionsToFluxScript(taskOptions) // option task = { ... }
    const preamble = `${varOption}\n\n${taskOption}`
    const script = addDestinationToFluxScript(newScript, taskOptions)

    saveNewScript(script, preamble)
  }

  private handleChangeToBucketName = (bucketName: string) => {
    const {setTaskOption} = this.props

    setTaskOption({key: 'toBucketName', value: bucketName})
  }

  private handleChangeScheduleType = (taskScheduleType: TaskSchedule) => {
    const {setTaskOption} = this.props

    setTaskOption({key: 'taskScheduleType', value: taskScheduleType})
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const {setTaskOption} = this.props

    const key = e.target.name as TaskOptionKeys
    const value = e.target.value

    setTaskOption({key, value})
  }
}

const mstp = (state: AppState): StateProps => {
  const {newScript, taskOptions} = state.resources.tasks
  const {timeRange} = getActiveTimeMachine(state)
  const activeQuery = getActiveQuery(state)
  const org = getOrg(state)
  const userDefinedVars = getVariableAssignments(
    state,
    state.timeMachines.activeTimeMachineID
  )

  return {
    newScript,
    taskOptions: {...taskOptions, toOrgName: org.name},
    timeRange,
    activeQuery,
    userDefinedVars,
  }
}

const mdtp: DispatchProps = {
  saveNewScript,
  setTaskOption,
  clearTask,
  setNewScript,
  refreshTimeMachineVariableValues,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(SaveAsTaskForm))
