// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import TaskForm from 'src/tasks/components/TaskForm'
import TaskHeader from 'src/tasks/components/TaskHeader'
import {Page} from '@influxdata/clockface'

import FluxEditor from 'src/shared/components/FluxMonacoEditor'

// Actions
import {
  setNewScript,
  setTaskOption,
  clearTask,
} from 'src/tasks/actions/creators'
import {saveNewScript, cancel} from 'src/tasks/actions/thunks'

// Utils
import {
  taskOptionsToFluxScript,
  addDestinationToFluxScript,
} from 'src/utils/taskOptionsToFluxScript'
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {AppState, TaskOptionKeys, TaskSchedule} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

class TaskPage extends PureComponent<Props> {
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
    const {newScript, taskOptions} = this.props

    return (
      <Page titleTag={pageTitleSuffixer(['Create Task'])}>
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
                taskOptions={taskOptions}
                canSubmit={this.isFormValid}
                onChangeInput={this.handleChangeInput}
                onChangeScheduleType={this.handleChangeScheduleType}
              />
            </div>
            <div className="task-form--editor">
              <FluxEditor
                script={newScript}
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
      newScript,
    } = this.props

    const hasSchedule = !!cron || !!interval
    return hasSchedule && !!name && !!newScript
  }

  private handleChangeScript = (script: string) => {
    this.props.setNewScript(script)
  }

  private handleChangeScheduleType = (value: TaskSchedule) => {
    this.props.setTaskOption({key: 'taskScheduleType', value})
  }

  private handleSave = () => {
    const {newScript, taskOptions} = this.props

    const taskOption: string = taskOptionsToFluxScript(taskOptions)
    const script: string = addDestinationToFluxScript(newScript, taskOptions)
    const preamble = `${taskOption}`

    this.props.saveNewScript(script, preamble)
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

const mstp = (state: AppState) => {
  const {tasks} = state.resources
  const {taskOptions, newScript} = tasks

  return {
    taskOptions,
    newScript,
  }
}

const mdtp = {
  setNewScript,
  saveNewScript,
  setTaskOption,
  clearTask,
  cancel,
}

const connector = connect(mstp, mdtp)

export default connector(TaskPage)
