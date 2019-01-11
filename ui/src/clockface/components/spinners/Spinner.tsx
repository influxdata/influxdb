// Libraries
import React, {Component} from 'react'

// Types
import {RemoteDataState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  loading: RemoteDataState
  children: JSX.Element[] | JSX.Element
}

@ErrorHandling
export default class Spinner extends Component<Props> {
  public render() {
    return this.children
  }

  private get children(): JSX.Element | JSX.Element[] {
    const {loading, children} = this.props

    if (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
    ) {
      return (
        <div className="spinner-container">
          <div className="spinner" />
        </div>
      )
    }

    return children
  }
}
