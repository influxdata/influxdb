// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import TaskForm from 'src/tasks/components/TaskForm'

// Actions
import {
  saveNewScript,
  setTaskOption,
  clearTask,
  setNewScript,
  setTaskToken,
} from 'src/tasks/actions'
import {getAuthorizations} from 'src/authorizations/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {formatVarsOption} from 'src/variables/utils/formatVarsOption'
import {
  taskOptionsToFluxScript,
  addDestinationToFluxScript,
} from 'src/utils/taskOptionsToFluxScript'

// Types
import {AppState, TimeRange, RemoteDataState} from 'src/types'
import {
  TaskSchedule,
  TaskOptions,
  TaskOptionKeys,
} from 'src/utils/taskOptionsToFluxScript'
import {DashboardDraftQuery} from 'src/types/dashboards'
import {Authorization} from '@influxdata/influx'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

interface OwnProps {
  dismiss: () => void
}

interface DispatchProps {
  saveNewScript: typeof saveNewScript
  setTaskOption: typeof setTaskOption
  clearTask: typeof clearTask
  setNewScript: typeof setNewScript
  getTokens: typeof getAuthorizations
  setTaskToken: typeof setTaskToken
}

interface StateProps {
  taskOptions: TaskOptions
  draftQueries: DashboardDraftQuery[]
  activeQueryIndex: number
  newScript: string
  timeRange: TimeRange
  tokens: Authorization[]
  tokenStatus: RemoteDataState
  selectedToken: Authorization
}

type Props = StateProps & OwnProps & DispatchProps

class SaveAsTaskForm extends PureComponent<Props & WithRouterProps> {
  public async componentDidMount() {
    const {setTaskOption, setNewScript} = this.props

    setTaskOption({
      key: 'taskScheduleType',
      value: TaskSchedule.interval,
    })

    setNewScript(this.activeScript)
    await this.props.getTokens()
    this.props.setTaskToken(this.props.tokens[0])
  }

  public componentWillUnmount() {
    const {clearTask} = this.props

    clearTask()
  }

  public render() {
    const {
      taskOptions,
      dismiss,
      tokens,
      tokenStatus,
      selectedToken,
    } = this.props

    return (
      <SpinnerContainer
        loading={tokenStatus}
        spinnerComponent={<TechnoSpinner />}
      >
        <TaskForm
          taskOptions={taskOptions}
          onChangeScheduleType={this.handleChangeScheduleType}
          onChangeInput={this.handleChangeInput}
          onChangeToBucketName={this.handleChangeToBucketName}
          isInOverlay={true}
          onSubmit={this.handleSubmit}
          canSubmit={this.isFormValid}
          dismiss={dismiss}
          tokens={tokens}
          selectedToken={selectedToken}
          onTokenChange={this.handleTokenChange}
        />
      </SpinnerContainer>
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
    const {
      saveNewScript,
      newScript,
      taskOptions,
      timeRange,
      selectedToken,
    } = this.props

    // When a task runs, it does not have access to variables that we typically
    // inject into the script via the front end. So any variables that are used
    // in the script need to be embedded in the script text itself before
    // saving it as a task
    //
    // TODO(chnn): Embed user-defined variables in the script as well
    const timeRangeVars = getTimeRangeVars(timeRange)
    const windowPeriodVars = await getWindowVars(newScript, timeRangeVars)

    // Don't embed variables that are not used in the script
    const vars = [...timeRangeVars, ...windowPeriodVars].filter(assignment =>
      newScript.includes(assignment.id.name)
    )

    const varOption: string = formatVarsOption(vars) // option v = { ... }
    const taskOption: string = taskOptionsToFluxScript(taskOptions) // option task = { ... }
    const preamble = `${varOption}\n\n${taskOption}`
    const script = addDestinationToFluxScript(newScript, taskOptions)

    saveNewScript(script, preamble, selectedToken.token)
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

  private handleTokenChange = (selectedToken: Authorization) => {
    this.props.setTaskToken(selectedToken)
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    tasks: {newScript, taskOptions, taskToken},
    tokens,
    orgs: {org},
  } = state

  const {draftQueries, activeQueryIndex, timeRange} = getActiveTimeMachine(
    state
  )

  return {
    newScript,
    taskOptions: {...taskOptions, toOrgName: org.name},
    timeRange,
    draftQueries,
    activeQueryIndex,
    tokens: tokens.list,
    tokenStatus: tokens.status,
    selectedToken: taskToken,
  }
}

const mdtp: DispatchProps = {
  saveNewScript,
  setTaskOption,
  clearTask,
  setNewScript,
  getTokens: getAuthorizations,
  setTaskToken,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(SaveAsTaskForm))
