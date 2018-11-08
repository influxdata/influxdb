import React, {PureComponent} from 'react'
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import {RemoteDataState, FluxTable} from 'src/types'

interface Props {
  error: Error
  isInitialFetch: boolean
  loading: RemoteDataState
  tables: FluxTable[]
}

export default class EmptyRefreshingView extends PureComponent<Props> {
  public render() {
    const {error, isInitialFetch, loading, tables} = this.props
    if (error) {
      return <EmptyGraphMessage message={`Error: ${error.message}`} />
    }

    if (isInitialFetch && loading === RemoteDataState.Loading) {
      return <EmptyGraphMessage message="Loading..." />
    }

    if (!tables.some(d => !!d.data.length)) {
      return <EmptyGraphMessage message="No Results" />
    }

    return this.props.children
  }
}
