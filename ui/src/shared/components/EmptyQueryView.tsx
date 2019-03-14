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
  errorMessage: string
  isInitialFetch: boolean
  loading: RemoteDataState
  tables: FluxTable[]
  queries: DashboardQuery[]
  fallbackNote?: string
}

export default class EmptyQueryView extends PureComponent<Props> {
  public render() {
    const {
      errorMessage,
      isInitialFetch,
      loading,
      tables,
      queries,
      fallbackNote,
    } = this.props

    if (loading === RemoteDataState.NotStarted || !queries.length) {
      return (
        <EmptyGraphMessage
          message={emptyGraphCopy}
          testID="empty-graph--no-queries"
        />
      )
    }

    if (errorMessage) {
      return (
        <EmptyGraphMessage
          message={`Error: ${errorMessage}`}
          testID="empty-graph--error"
        />
      )
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
      return (
        <EmptyGraphMessage
          message="No Results"
          testID="empty-graph--no-results"
        />
      )
    }

    return this.props.children
  }
}
