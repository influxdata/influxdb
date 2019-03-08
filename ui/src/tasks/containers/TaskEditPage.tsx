// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// Components
import TaskForm from 'src/tasks/components/TaskForm'
import TaskHeader from 'src/tasks/components/TaskHeader'
import {Page} from 'src/pageLayout'
import FluxEditor from 'src/shared/components/FluxEditor'

// Actions
import {
  updateScript,
  selectTaskByID,
  setCurrentScript,
  cancel,
  setTaskOption,
  clearTask,
  setAllTaskOptions,
} from 'src/tasks/actions/v2'

// Types
import {Organization, ITask as Task} from '@influxdata/influx'
import {State as TasksState} from 'src/tasks/reducers/v2'
import {
  TaskOptions,
  TaskOptionKeys,
  TaskSchedule,
} from 'src/utils/taskOptionsToFluxScript'
import {Links} from 'src/types/v2'

interface PassedInProps {
  router: InjectedRouter
  params: {id: string}
}

interface ConnectStateProps {
  orgs: Organization[]
  taskOptions: TaskOptions
  currentTask: Task
  currentScript: string
  tasksLink: string
}

interface ConnectDispatchProps {
  setTaskOption: typeof setTaskOption
  setCurrentScript: typeof setCurrentScript
  updateScript: typeof updateScript
  cancel: typeof cancel
  selectTaskByID: typeof selectTaskByID
  clearTask: typeof clearTask
  setAllTaskOptions: typeof setAllTaskOptions
}

class TaskEditPage extends PureComponent<
  PassedInProps & ConnectStateProps & ConnectDispatchProps
> {
  constructor(props) {
    super(props)
  }

  public async componentDidMount() {
    const {
      params: {id},
    } = this.props
    await this.props.selectTaskByID(id)

    const {currentTask} = this.props

    this.props.setAllTaskOptions(currentTask)
  }

  public componentWillUnmount() {
    this.props.clearTask()
  }

  public render(): JSX.Element {
    const {currentScript, taskOptions, orgs} = this.props

    return (
      <Page titleTag={`Edit ${taskOptions.name}`}>
        <TaskHeader
          title="Update Task"
          canSubmit={this.isFormValid}
          onCancel={this.handleCancel}
          onSave={this.handleSave}
        />
        <Page.Contents fullWidth={true} scrollable={false}>
          <div className="task-form">
            <div className="task-form--options">
              <TaskForm
                orgs={orgs}
                canSubmit={this.isFormValid}
                taskOptions={taskOptions}
                onChangeInput={this.handleChangeInput}
                onChangeScheduleType={this.handleChangeScheduleType}
                onChangeTaskOrgID={this.handleChangeTaskOrgID}
              />
            </div>
            <div className="task-form--editor">
              <FluxEditor
                script={currentScript}
                onChangeScript={this.handleChangeScript}
                visibility="visible"
                suggestions={[]}
              />
            </div>
          </div>
        </Page.Contents>
      </Page>
    )
  }

  private get isFormValid(): boolean {
    const {
      taskOptions: {name, cron, interval},
      currentScript,
    } = this.props

    const hasSchedule = !!cron || !!interval
    return hasSchedule && !!name && !!currentScript
  }

  private handleChangeScript = (script: string) => {
    this.props.setCurrentScript(script)
  }

  private handleChangeScheduleType = (schedule: TaskSchedule) => {
    this.props.setTaskOption({key: 'taskScheduleType', value: schedule})
  }

  private handleSave = () => {
    this.props.updateScript()
  }

  private handleCancel = () => {
    this.props.cancel()
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const {name, value} = e.target
    const key = name as TaskOptionKeys

    this.props.setTaskOption({key, value})
  }

  private handleChangeTaskOrgID = (orgID: string) => {
    this.props.setTaskOption({key: 'orgID', value: orgID})
  }
}

const mstp = ({
  tasks,
  links,
  orgs,
}: {
  tasks: TasksState
  links: Links
  orgs: Organization[]
}): ConnectStateProps => {
  return {
    orgs,
    taskOptions: tasks.taskOptions,
    currentScript: tasks.currentScript,
    tasksLink: links.tasks,
    currentTask: tasks.currentTask,
  }
}

const mdtp: ConnectDispatchProps = {
  setTaskOption,
  setCurrentScript,
  updateScript,
  cancel,
  selectTaskByID,
  setAllTaskOptions,
  clearTask,
}

export default connect<ConnectStateProps, ConnectDispatchProps, {}>(
  mstp,
  mdtp
)(TaskEditPage)
