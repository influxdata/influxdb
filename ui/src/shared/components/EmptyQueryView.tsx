// Libraries
import React, {PureComponent} from 'react'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import Markdown from 'src/shared/components/views/Markdown'

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
  fallbackNote?: string
}

export default class EmptyQueryView extends PureComponent<Props> {
  public render() {
    const {
      error,
      isInitialFetch,
      loading,
      tables,
      queries,
      fallbackNote,
    } = this.props

    if (loading === RemoteDataState.NotStarted) {
      return <EmptyGraphMessage message={emptyGraphCopy} />
    }

    if (!queries.length) {
      return <EmptyGraphMessage message={emptyGraphCopy} />
    }

    if (error) {
      return <EmptyGraphMessage message={`Error: ${error.message}`} />
    }

    const hasNoResults = tables.every(d => !d.data.length)

    if (
      (isInitialFetch || hasNoResults) &&
      loading === RemoteDataState.Loading
    ) {
      return <EmptyGraphMessage message="Loading..." />
    }

    if (hasNoResults && fallbackNote) {
      return <Markdown text={fallbackNote} />
    }

    if (hasNoResults) {
      return <EmptyGraphMessage message="No Results" />
    }

    return this.props.children
  }
}
