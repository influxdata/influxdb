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
import {
  setNewScript,
  saveNewScript,
  setTaskOption,
  clearTask,
  cancel,
  setTaskToken,
} from 'src/tasks/actions'

// Utils
import {
  taskOptionsToFluxScript,
  addDestinationToFluxScript,
} from 'src/utils/taskOptionsToFluxScript'

import {AppState} from 'src/types'
import {
  TaskOptions,
  TaskOptionKeys,
  TaskSchedule,
} from 'src/utils/taskOptionsToFluxScript'
import {getAuthorizations} from 'src/authorizations/actions'
import {Authorization} from '@influxdata/influx'
import {
  RemoteDataState,
  SpinnerContainer,
  TechnoSpinner,
} from '@influxdata/clockface'

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectStateProps {
  taskOptions: TaskOptions
  newScript: string
  tokens: Authorization[]
  tokenStatus: RemoteDataState
  selectedToken: Authorization
}

interface ConnectDispatchProps {
  setNewScript: typeof setNewScript
  saveNewScript: typeof saveNewScript
  setTaskOption: typeof setTaskOption
  clearTask: typeof clearTask
  cancel: typeof cancel
  getTokens: typeof getAuthorizations
  setTaskToken: typeof setTaskToken
}

class TaskPage extends PureComponent<
  PassedInProps & ConnectStateProps & ConnectDispatchProps
> {
  constructor(props) {
    super(props)
  }
  public async componentDidMount() {
    this.props.setTaskOption({
      key: 'taskScheduleType',
      value: TaskSchedule.interval,
    })
    await this.props.getTokens()
    this.props.setTaskToken(this.props.tokens[0])
  }

  public componentWillUnmount() {
    this.props.clearTask()
  }

  public render(): JSX.Element {
    const {
      newScript,
      taskOptions,
      tokens,
      tokenStatus,
      selectedToken,
    } = this.props

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
              <SpinnerContainer
                loading={tokenStatus}
                spinnerComponent={<TechnoSpinner />}
              >
                <TaskForm
                  taskOptions={taskOptions}
                  canSubmit={this.isFormValid}
                  onChangeInput={this.handleChangeInput}
                  onChangeScheduleType={this.handleChangeScheduleType}
                  tokens={tokens}
                  selectedToken={selectedToken}
                  onTokenChange={this.handleTokenChange}
                />
              </SpinnerContainer>
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
    const {newScript, taskOptions, selectedToken} = this.props

    const taskOption: string = taskOptionsToFluxScript(taskOptions)
    const script: string = addDestinationToFluxScript(newScript, taskOptions)
    const preamble = `${taskOption}`

    this.props.saveNewScript(script, preamble, selectedToken.token)
  }

  private handleCancel = () => {
    this.props.cancel()
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const {name, value} = e.target
    const key = name as TaskOptionKeys

    this.props.setTaskOption({key, value})
  }

  private handleTokenChange = (selectedToken: Authorization) => {
    this.props.setTaskToken(selectedToken)
  }
}

const mstp = ({tasks, tokens}: AppState): ConnectStateProps => {
  return {
    taskOptions: tasks.taskOptions,
    newScript: tasks.newScript,
    tokens: tokens.list,
    tokenStatus: tokens.status,
    selectedToken: tasks.taskToken,
  }
}

const mdtp: ConnectDispatchProps = {
  setNewScript,
  saveNewScript,
  setTaskOption,
  clearTask,
  cancel,
  getTokens: getAuthorizations,
  setTaskToken,
}

export default connect<ConnectStateProps, ConnectDispatchProps, {}>(
  mstp,
  mdtp
)(TaskPage)
