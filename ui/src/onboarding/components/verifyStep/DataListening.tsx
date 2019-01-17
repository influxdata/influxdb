// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Apis
import {executeQuery} from 'src/shared/apis/v2/query'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from 'src/clockface'
import ConnectionInformation, {
  LoadingState,
} from 'src/onboarding/components/verifyStep/ConnectionInformation'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {InfluxLanguage} from 'src/types/v2/dashboards'

export interface Props {
  bucket: string
  stepIndex: number
  onSetStepStatus: (index: number, status: StepStatus) => void
}

interface State {
  loading: LoadingState
  timePassedInSeconds: number
  secondsLeft: number
}

const MINUTE = 60000
const FETCH_WAIT = 5000
const SECONDS = 60
const TIMER_WAIT = 1000

@ErrorHandling
class DataListening extends PureComponent<Props, State> {
  private intervalID: NodeJS.Timer
  private startTime: number
  private timer: NodeJS.Timer

  constructor(props: Props) {
    super(props)

    this.state = {
      loading: LoadingState.NotStarted,
      timePassedInSeconds: 0,
      secondsLeft: SECONDS,
    }
  }

  public componentWillUnmount() {
    clearInterval(this.intervalID)
    clearInterval(this.timer)
    this.setState({
      timePassedInSeconds: 0,
      secondsLeft: SECONDS,
    })
  }

  public render() {
    return (
      <div className="wizard-step--body-streaming">
        {this.connectionInfo}
        {this.listenButton}
      </div>
    )
  }

  private get connectionInfo(): JSX.Element {
    const {loading} = this.state

    if (loading === LoadingState.NotStarted) {
      return
    }

    return (
      <ConnectionInformation
        loading={this.state.loading}
        bucket={this.props.bucket}
        countDownSeconds={this.state.secondsLeft}
      />
    )
  }

  private get listenButton(): JSX.Element {
    const {loading} = this.state

    if (loading === LoadingState.Loading || loading === LoadingState.Done) {
      return
    }

    return (
      <Button
        color={ComponentColor.Primary}
        text="Listen for Data"
        size={ComponentSize.Medium}
        onClick={this.handleClick}
        status={ComponentStatus.Default}
        titleText={'Listen for Data'}
      />
    )
  }

  private handleClick = (): void => {
    this.startTimer()
    this.setState({loading: LoadingState.Loading})
    this.startTime = Number(new Date())
    this.checkForData()
  }

  private checkForData = async (): Promise<void> => {
    const {bucket, onSetStepStatus, stepIndex} = this.props
    const {secondsLeft} = this.state
    const script = `from(bucket: "${bucket}")
      |> range(start: -1m)`

    let rowCount
    let timePassed

    try {
      const response = await executeQuery(
        '/api/v2/query',
        script,
        InfluxLanguage.Flux
      ).promise
      rowCount = response.rowCount
      timePassed = Number(new Date()) - this.startTime
    } catch (err) {
      this.setState({loading: LoadingState.Error})
      onSetStepStatus(stepIndex, StepStatus.Incomplete)
      return
    }

    if (rowCount > 1) {
      this.setState({loading: LoadingState.Done})
      onSetStepStatus(stepIndex, StepStatus.Complete)
      return
    }

    if (timePassed >= MINUTE || secondsLeft <= 0) {
      this.setState({loading: LoadingState.NotFound})
      onSetStepStatus(stepIndex, StepStatus.Incomplete)
      return
    }
    this.intervalID = setTimeout(this.checkForData, FETCH_WAIT)
  }

  private startTimer() {
    this.setState({timePassedInSeconds: 0, secondsLeft: SECONDS})

    this.timer = setInterval(this.countDown, TIMER_WAIT)
  }

  private countDown = () => {
    const {secondsLeft} = this.state
    const secs = secondsLeft - 1
    this.setState({
      timePassedInSeconds: SECONDS - secs,
      secondsLeft: secs,
    })

    if (secs === 0) {
      clearInterval(this.timer)
    }
  }
}

export default DataListening
