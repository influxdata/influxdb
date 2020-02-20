// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'
import {connect} from 'react-redux'

// Components
import {SparkleSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types'

interface StateProps {
  status: RemoteDataState
  errorMessage: string
}

type Props = StateProps

export class StatusIndicator extends PureComponent<Props> {
  public render() {
    const {status} = this.props
    return (
      <div className="line-protocol--spinner">
        <p data-testid="line-protocol--status" className={this.statusClassName}>
          {this.statusText.status}
        </p>
        <SparkleSpinner loading={status} sizePixels={220} />
        <p className={this.statusClassName}>{this.statusText.message}</p>
      </div>
    )
  }

  private get statusClassName(): string {
    const {status} = this.props

    return classnames(`line-protocol--status`, {
      loading: status === RemoteDataState.Loading,
      success: status === RemoteDataState.Done,
      error: status === RemoteDataState.Error,
    })
  }

  private get statusText() {
    let status = ''
    let message = ''
    switch (this.props.status) {
      case RemoteDataState.Loading:
        status = 'Loading...'
        message = 'Just a moment'
        break
      case RemoteDataState.Done:
        status = 'Data Written Successfully'
        message = 'Hooray!'
        break
      case RemoteDataState.Error:
        status = 'Unable to Write Data'
        message = `Error: ${this.props.errorMessage}`
        break
    }

    return {
      status,
      message,
    }
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {lpStatus, lpError},
  },
}: AppState): StateProps => ({
  status: lpStatus,
  errorMessage: lpError,
})

export default connect<StateProps, {}, {}>(mstp, null)(StatusIndicator)
