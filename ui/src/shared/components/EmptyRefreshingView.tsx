// Libraries
import React, {PureComponent} from 'react'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Constants
import {emptyGraphCopy} from 'src/shared/copy/cell'

// Types
import {RemoteDataState, FluxTable} from 'src/types'
import {DashboardQuery} from 'src/types/v2'

interface Props {
  error: Error
  isInitialFetch: boolean
  loading: RemoteDataState
  tables: FluxTable[]
  queries: DashboardQuery[]
}

export default class EmptyRefreshingView extends PureComponent<Props> {
  public render() {
    const {error, isInitialFetch, loading, tables, queries} = this.props

    if (!queries.length) {
      return <EmptyGraphMessage message={emptyGraphCopy} />
    }

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
