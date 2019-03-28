// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// Components
import TaskForm from 'src/tasks/components/TaskForm'
import TaskHeader from 'src/tasks/components/TaskHeader'
import FluxEditor from 'src/shared/components/FluxEditor'
import {Page} from 'src/pageLayout'

// Actions
import {State as TasksState} from 'src/tasks/reducers'
import {
  setNewScript,
  saveNewScript,
  setTaskOption,
  clearTask,
  cancel,
} from 'src/tasks/actions'

// Utils
import {
  taskOptionsToFluxScript,
  addDestinationToFluxScript,
} from 'src/utils/taskOptionsToFluxScript'

// Types
import {Links} from 'src/types/links'
import {Organization} from 'src/types'
import {
  TaskOptions,
  TaskOptionKeys,
  TaskSchedule,
} from 'src/utils/taskOptionsToFluxScript'

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectStateProps {
  orgs: Organization[]
  taskOptions: TaskOptions
  newScript: string
  tasksLink: string
}

interface ConnectDispatchProps {
  setNewScript: typeof setNewScript
  saveNewScript: typeof saveNewScript
  setTaskOption: typeof setTaskOption
  clearTask: typeof clearTask
  cancel: typeof cancel
}

class TaskPage extends PureComponent<
  PassedInProps & ConnectStateProps & ConnectDispatchProps
> {
  constructor(props) {
    super(props)
  }

  public componentDidMount() {
    this.props.setTaskOption({
      key: 'taskScheduleType',
      value: TaskSchedule.interval,
    })
  }

  public componentWillUnmount() {
    this.props.clearTask()
  }

  public render(): JSX.Element {
    const {newScript, taskOptions, orgs} = this.props

    return (
      <Page titleTag="Create Task">
        <TaskHeader
          title="Create Task"
          canSubmit={this.isFormValid}
          onCancel={this.handleCancel}
          onSave={this.handleSave}
        />
        <Page.Contents fullWidth={true} scrollable={false}>
          <div className="task-form">
            <div className="task-form--options">
              <TaskForm
                orgs={orgs}
                taskOptions={taskOptions}
                canSubmit={this.isFormValid}
                onChangeInput={this.handleChangeInput}
                onChangeScheduleType={this.handleChangeScheduleType}
                onChangeTaskOrgID={this.handleChangeTaskOrgID}
              />
            </div>
            <div className="task-form--editor">
              <FluxEditor
                script={newScript}
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
      newScript,
    } = this.props

    const hasSchedule = !!cron || !!interval
    return hasSchedule && !!name && !!newScript
  }

  private handleChangeScript = (script: string) => {
    this.props.setNewScript(script)
  }

  private handleChangeScheduleType = (schedule: TaskSchedule) => {
    this.props.setTaskOption({key: 'taskScheduleType', value: schedule})
  }

  private handleSave = () => {
    const {newScript, taskOptions} = this.props

    const taskOption: string = taskOptionsToFluxScript(taskOptions)
    const script: string = addDestinationToFluxScript(newScript, taskOptions)
    const preamble = `${taskOption}`

    this.props.saveNewScript(script, preamble, this.orgName)
  }

  private get orgName(): string {
    return this.props.orgs[0].name
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
    newScript: tasks.newScript,
    tasksLink: links.tasks,
  }
}

const mdtp: ConnectDispatchProps = {
  setNewScript,
  saveNewScript,
  setTaskOption,
  clearTask,
  cancel,
}

export default connect<ConnectStateProps, ConnectDispatchProps, {}>(
  mstp,
  mdtp
)(TaskPage)
