// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {InjectedRouter} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import TaskForm from 'src/tasks/components/TaskForm'
import TaskHeader from 'src/tasks/components/TaskHeader'
import {Page} from '@influxdata/clockface'

import FluxEditor from 'src/shared/components/FluxMonacoEditor'

// Actions
import {
  setCurrentScript,
  setTaskOption,
  clearTask,
} from 'src/tasks/actions/creators'
import {
  updateScript,
  selectTaskByID,
  cancel,
  setAllTaskOptionsByID,
} from 'src/tasks/actions/thunks'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {
  AppState,
  Task,
  TaskOptions,
  TaskOptionKeys,
  TaskSchedule,
} from 'src/types'

interface OwnProps {
  router: InjectedRouter
  params: {id: string}
}

interface StateProps {
  taskOptions: TaskOptions
  currentTask: Task
  currentScript: string
}

interface DispatchProps {
  setTaskOption: typeof setTaskOption
  setCurrentScript: typeof setCurrentScript
  updateScript: typeof updateScript
  cancel: typeof cancel
  selectTaskByID: typeof selectTaskByID
  clearTask: typeof clearTask
  setAllTaskOptionsByID: typeof setAllTaskOptionsByID
}

type Props = OwnProps & StateProps & DispatchProps

class TaskEditPage extends PureComponent<Props> {
  constructor(props) {
    super(props)
  }

  public componentDidMount() {
    const {
      params: {id},
    } = this.props
    this.props.selectTaskByID(id)
    this.props.setAllTaskOptionsByID(id)
  }

  public componentWillUnmount() {
    this.props.clearTask()
  }

  public render(): JSX.Element {
    const {currentScript, taskOptions} = this.props

    return (
      <Page titleTag={pageTitleSuffixer([`Edit ${taskOptions.name}`])}>
        <TaskHeader
          title="Edit Task"
          canSubmit={this.isFormValid}
          onCancel={this.handleCancel}
          onSave={this.handleSave}
        />
        <Page.Contents fullWidth={true} scrollable={false}>
          <div className="task-form">
            <div className="task-form--options">
              <TaskForm
                canSubmit={this.isFormValid}
                taskOptions={taskOptions}
                onChangeInput={this.handleChangeInput}
                onChangeScheduleType={this.handleChangeScheduleType}
              />
            </div>
            <div className="task-form--editor">
              <FluxEditor
                script={currentScript}
                onChangeScript={this.handleChangeScript}
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
}

const mstp = (state: AppState): StateProps => {
  const {taskOptions, currentScript, currentTask} = state.resources.tasks

  return {
    taskOptions,
    currentScript,
    currentTask,
  }
}

const mdtp: DispatchProps = {
  setTaskOption,
  setCurrentScript,
  updateScript,
  cancel,
  selectTaskByID,
  setAllTaskOptionsByID,
  clearTask,
}

export default connect<StateProps, DispatchProps, {}>(mstp, mdtp)(TaskEditPage)
