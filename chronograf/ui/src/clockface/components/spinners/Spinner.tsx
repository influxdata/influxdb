// Libraries
import React, {Component} from 'react'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  loading: RemoteDataState
  children: JSX.Element[] | JSX.Element
}

class Spinner extends Component<Props> {
  public render() {
    return <div className="spinner-container">{this.children}</div>
  }

  private get children(): JSX.Element | JSX.Element[] {
    const {loading, children} = this.props

    if (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
    ) {
      return <div className="spinner" />
    }

    return children
  }
}

export default Spinner
