// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import TaskForm from 'src/tasks/components/TaskForm'

// Actions
import {
  saveNewScript,
  setTaskOption,
  clearTask,
  setNewScript,
} from 'src/tasks/actions/v2'

// Utils
import {timeRangeVariables} from 'src/shared/utils/timeRangeVariables'
import {renderQuery} from 'src/shared/utils/renderQuery'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState, Organization, TimeRange} from 'src/types/v2'
import {
  TaskSchedule,
  TaskOptions,
  TaskOptionKeys,
} from 'src/utils/taskOptionsToFluxScript'
import {DashboardDraftQuery} from 'src/types/v2/dashboards'

interface OwnProps {
  dismiss: () => void
}

interface DispatchProps {
  saveNewScript: typeof saveNewScript
  setTaskOption: typeof setTaskOption
  clearTask: typeof clearTask
  setNewScript: typeof setNewScript
}

interface StateProps {
  orgs: Organization[]
  taskOptions: TaskOptions
  draftQueries: DashboardDraftQuery[]
  activeQueryIndex: number
  newScript: string
  timeRange: TimeRange
}

type Props = StateProps & OwnProps & DispatchProps

class SaveAsTaskForm extends PureComponent<Props> {
  public componentDidMount() {
    const {setTaskOption, setNewScript} = this.props

    setTaskOption({
      key: 'taskScheduleType',
      value: TaskSchedule.interval,
    })

    setNewScript(this.activeScript)
  }

  public componentWillUnmount() {
    const {clearTask} = this.props

    clearTask()
  }

  public render() {
    const {orgs, taskOptions, dismiss} = this.props

    return (
      <TaskForm
        orgs={orgs}
        taskOptions={taskOptions}
        onChangeScheduleType={this.handleChangeScheduleType}
        onChangeInput={this.handleChangeInput}
        onChangeTaskOrgID={this.handleChangeTaskOrgID}
        onChangeToOrgName={this.handleChangeToOrgName}
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
    const {draftQueries, activeQueryIndex} = this.props

    return _.get(draftQueries, `${activeQueryIndex}.text`)
  }

  private handleSubmit = async () => {
    const {saveNewScript, newScript, taskOptions, timeRange} = this.props

    const script = await renderQuery(newScript, timeRangeVariables(timeRange))

    saveNewScript(script, taskOptions)
  }

  private handleChangeTaskOrgID = (orgID: string) => {
    const {setTaskOption} = this.props

    setTaskOption({key: 'orgID', value: orgID})
  }

  private handleChangeToOrgName = (orgName: string) => {
    const {setTaskOption} = this.props

    setTaskOption({key: 'toOrgName', value: orgName})
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
  const {
    orgs,
    tasks: {newScript, taskOptions},
  } = state

  const {draftQueries, activeQueryIndex, timeRange} = getActiveTimeMachine(
    state
  )

  return {
    orgs,
    newScript,
    taskOptions,
    timeRange,
    draftQueries,
    activeQueryIndex,
  }
}

const mdtp: DispatchProps = {
  saveNewScript,
  setTaskOption,
  clearTask,
  setNewScript,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(SaveAsTaskForm)
