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
import ConnectionInformation from 'src/onboarding/components/configureStep/streaming/ConnectionInformation'

// types
import {RemoteDataState} from 'src/types'
import {InfluxLanguage} from 'src/types/v2/dashboards'

export interface Props {
  bucket: string
}

interface State {
  loading: RemoteDataState
}

const MINUTE = 60000
const WAIT = 5000

@ErrorHandling
class DataListening extends PureComponent<Props, State> {
  private intervalID: NodeJS.Timer
  private startTime: number

  constructor(props: Props) {
    super(props)

    this.state = {loading: RemoteDataState.NotStarted}
  }

  public componentWillUnmount() {
    clearInterval(this.intervalID)
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

    if (loading === RemoteDataState.NotStarted) {
      return
    }

    return (
      <ConnectionInformation
        loading={this.state.loading}
        bucket={this.props.bucket}
      />
    )
  }
  private get listenButton(): JSX.Element {
    const {loading} = this.state

    if (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.Done
    ) {
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
    this.setState({loading: RemoteDataState.Loading})
    this.startTime = Number(new Date())
    this.checkForData()
  }

  private checkForData = async (): Promise<void> => {
    const {bucket} = this.props
    const script = `from(bucket: "${bucket}")
      |> range(start: -1m)`

    let rowCount
    let timePassed

    try {
      const response = await executeQuery(
        '/api/v2/query',
        script,
        InfluxLanguage.Flux
      )
      rowCount = response.rowCount
      timePassed = Number(new Date()) - this.startTime
    } catch (err) {
      this.setState({loading: RemoteDataState.Error})
      return
    }

    if (rowCount > 1) {
      this.setState({loading: RemoteDataState.Done})
      return
    }

    if (timePassed >= MINUTE) {
      this.setState({loading: RemoteDataState.Error})
      return
    }

    this.intervalID = setTimeout(this.checkForData, WAIT)
  }
}

export default DataListening
