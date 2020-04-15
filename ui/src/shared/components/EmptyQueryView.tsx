// Libraries
import React, {PureComponent} from 'react'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import EmptyGraphErrorTooltip from 'src/shared/components/EmptyGraphErrorTooltip'
import EmptyGraphError from 'src/shared/components/EmptyGraphError'
import ScrollableMarkdown from 'src/shared/components/views/ScrollableMarkdown'

// Constants
import {emptyGraphCopy} from 'src/shared/copy/cell'

// Types
import {RemoteDataState} from 'src/types'
import {DashboardQuery} from 'src/types'

export enum ErrorFormat {
  Tooltip = 'tooltip',
  Scroll = 'scroll',
}

interface Props {
  errorMessage: string
  errorFormat: ErrorFormat
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
      errorFormat,
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
      if (errorFormat === ErrorFormat.Tooltip)
        return (
          <EmptyGraphErrorTooltip
            message={errorMessage}
            testID="empty-graph--error"
          />
        )

      if (errorFormat === ErrorFormat.Scroll)
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
      return <ScrollableMarkdown text={fallbackNote} />
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
