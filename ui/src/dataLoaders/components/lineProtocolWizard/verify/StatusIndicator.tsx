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
      <>
        <div className="wizard-step--top-container">
          <div className="wizard-step--sparkle-container">
            <SparkleSpinner loading={status} />
          </div>
        </div>
        <div className="wizard-step--footer">
          <div className={this.footerClass} data-testid={this.footerClass}>
            {this.footerText}
            {this.errorMessage}
          </div>
        </div>
        <br />
      </>
    )
  }

  private get footerClass(): string {
    const {status} = this.props

    return classnames(`wizard-step--text-state`, {
      loading: status === RemoteDataState.Loading,
      success: status === RemoteDataState.Done,
      error: status === RemoteDataState.Error,
    })
  }

  private get footerText(): string {
    switch (this.props.status) {
      case RemoteDataState.Loading:
        return 'Loading...'
      case RemoteDataState.Done:
        return 'Data Written Successfully!'
      case RemoteDataState.Error:
        return 'Unable to Write Data'
    }
  }

  private get errorMessage(): JSX.Element {
    if (this.props.status === RemoteDataState.Error)
      return (
        <>
          <br />
          Error: {this.props.errorMessage}
        </>
      )
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

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(StatusIndicator)
