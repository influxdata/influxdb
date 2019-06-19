// Libraries
import React, {PureComponent} from 'react'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import EmptyGraphError from 'src/shared/components/EmptyGraphError'
import Markdown from 'src/shared/components/views/Markdown'

// Constants
import {emptyGraphCopy} from 'src/shared/copy/cell'

// Types
import {RemoteDataState} from 'src/types'
import {DashboardQuery} from 'src/types'

interface Props {
  errorMessage: string
  isInitialFetch: boolean
  loading: RemoteDataState
  hasResults: boolean
  queries: DashboardQuery[]
  fallbackNote?: string
}

export default class EmptyQueryView extends PureComponent<Props> {
  public render() {
    const {
      errorMessage,
      isInitialFetch,
      loading,
      queries,
      fallbackNote,
      hasResults,
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
        <EmptyGraphError message={errorMessage} testID="empty-graph--error" />
      )
    }

    if (
      (isInitialFetch || !hasResults) &&
      loading === RemoteDataState.Loading
    ) {
      return <EmptyGraphMessage message="Loading..." />
    }

    if (!hasResults && fallbackNote) {
      return <Markdown text={fallbackNote} />
    }

    if (!hasResults) {
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
