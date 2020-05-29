// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

export enum LoadingState {
  NotStarted = 'NotStarted',
  Loading = 'Loading',
  Done = 'Done',
  NotFound = 'NotFound',
  Error = 'Error',
}

export interface Props {
  loading: LoadingState
  bucket: string
  countDownSeconds: number
}

@ErrorHandling
class ConnectionInformation extends PureComponent<Props> {
  public render() {
    return (
      <div>
        <h4 className={`wizard-step--text-state ${this.className}`}>
          {this.header}
        </h4>
        <p>{this.additionalText}</p>
      </div>
    )
  }

  private get className(): string {
    switch (this.props.loading) {
      case LoadingState.Loading:
        return 'loading'
      case LoadingState.Done:
        return 'success'
      case LoadingState.NotFound:
      case LoadingState.Error:
        return 'error'
    }
  }

  private get header(): string {
    switch (this.props.loading) {
      case LoadingState.Loading:
        return 'Awaiting Connection...'
      case LoadingState.Done:
        return 'Connection Found!'
      case LoadingState.NotFound:
        return 'Data Not Found'
      case LoadingState.Error:
        return 'Error Listening for Data'
    }
  }

  private get additionalText(): string {
    switch (this.props.loading) {
      case LoadingState.Loading:
        return `Timeout in ${this.props.countDownSeconds} seconds`
      case LoadingState.Done:
        return `${this.props.bucket} is receiving data loud and clear!`
      case LoadingState.NotFound:
      case LoadingState.Error:
        return 'Check config and try again'
    }
  }
}

export default ConnectionInformation
