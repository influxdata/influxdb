import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

import TaskForm from 'src/tasks/components/TaskForm'
import TaskHeader from 'src/tasks/components/TaskHeader'
import {Task} from 'src/types/v2/tasks'
import {Page} from 'src/pageLayout'

import {Links} from 'src/types/v2/links'
import {State as TasksState} from 'src/tasks/reducers/v2'
import {
  updateScript,
  selectTaskByID,
  setCurrentScript,
  cancelUpdateTask,
  setTaskOption,
  setScheduleUnit,
} from 'src/tasks/actions/v2'
import {TaskOptions, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'
import {Organization} from 'src/types/v2'

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
  setScheduleUnit: typeof setScheduleUnit
}

class TaskPage extends PureComponent<
  PassedInProps & ConnectStateProps & ConnectDispatchProps
> {
  constructor(props) {
    super(props)
  }

  public componentDidMount() {
    this.props.selectTaskByID(this.props.params.id)
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
            onChangeScheduleUnit={this.handleChangeScheduleUnit}
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
    const {name: key, value} = e.target

    this.props.setTaskOption({key, value})
  }

  private handleChangeScheduleUnit = (unit: string, scheduleType: string) => {
    this.props.setScheduleUnit(unit, scheduleType)
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
  setScheduleUnit,
}

export default connect<ConnectStateProps, ConnectDispatchProps, {}>(
  mstp,
  mdtp
)(TaskPage)
