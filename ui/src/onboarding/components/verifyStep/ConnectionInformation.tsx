// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {RemoteDataState} from 'src/types'

export interface Props {
  loading: RemoteDataState
  bucket: string
  countDownSeconds: number
}

@ErrorHandling
class ListeningResults extends PureComponent<Props> {
  public render() {
    return (
      <>
        <h4 className={`wizard-step--text-state ${this.className}`}>
          {this.header}
        </h4>
        <p>{this.additionalText}</p>
      </>
    )
  }

  private get className(): string {
    switch (this.props.loading) {
      case RemoteDataState.Loading:
        return 'loading'
      case RemoteDataState.Done:
        return 'success'
      case RemoteDataState.Error:
        return 'error'
    }
  }

  private get header(): string {
    switch (this.props.loading) {
      case RemoteDataState.Loading:
        return 'Awaiting Connection...'
      case RemoteDataState.Done:
        return 'Connection Found!'
      case RemoteDataState.Error:
        return 'Connection Not Found'
    }
  }

  private get additionalText(): string {
    switch (this.props.loading) {
      case RemoteDataState.Loading:
        return `Timeout in ${this.props.countDownSeconds} seconds`
      case RemoteDataState.Done:
        return `${this.props.bucket} is receiving data loud and clear!`
      case RemoteDataState.Error:
        return 'Check config and try again'
    }
  }
}

export default ListeningResults
