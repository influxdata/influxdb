// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// Components
import TaskForm from 'src/tasks/components/TaskForm'
import TaskHeader from 'src/tasks/components/TaskHeader'
import {Page} from 'src/pageLayout'
import {Task as TaskAPI, User, Organization} from 'src/api'

// Actions
import {
  updateScript,
  selectTaskByID,
  setCurrentScript,
  cancelUpdateTask,
  setTaskOption,
  clearTask,
  setAllTaskOptions,
} from 'src/tasks/actions/v2'

// Types
import {Links} from 'src/types/v2/links'
import {State as TasksState} from 'src/tasks/reducers/v2'
import {
  TaskOptions,
  TaskOptionKeys,
  TaskSchedule,
} from 'src/utils/taskOptionsToFluxScript'

interface Task extends TaskAPI {
  organization: Organization
  owner?: User
  delay?: string
}

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
  cancelUpdateTask: typeof cancelUpdateTask
  selectTaskByID: typeof selectTaskByID
  clearTask: typeof clearTask
  setAllTaskOptions: typeof setAllTaskOptions
}

class TaskPage extends PureComponent<
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
      <Page>
        <TaskHeader
          title="Update Task"
          onCancel={this.handleCancel}
          onSave={this.handleSave}
        />
        <Page.Contents fullWidth={true} scrollable={false}>
          <TaskForm
            orgs={orgs}
            script={currentScript}
            taskOptions={taskOptions}
            onChangeScript={this.handleChangeScript}
            onChangeScheduleType={this.handleChangeScheduleType}
            onChangeInput={this.handleChangeInput}
            onChangeTaskOrgID={this.handleChangeTaskOrgID}
          />
        </Page.Contents>
      </Page>
    )
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
    this.props.cancelUpdateTask()
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
  cancelUpdateTask,
  selectTaskByID,
  setAllTaskOptions,
  clearTask,
}

export default connect<ConnectStateProps, ConnectDispatchProps, {}>(
  mstp,
  mdtp
)(TaskPage)
