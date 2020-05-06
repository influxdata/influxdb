import React, {FC} from 'react'
import {RemoteDataState} from 'src/types'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import EmptyGraphError from 'src/shared/components/EmptyGraphError'

// Constants
import {emptyGraphCopy} from 'src/shared/copy/cell'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
    status: RemoteDataState
    error: string | null
}

const EmptyQuery: FC<Props> = ({
    status,
    error
}) => {
    if (status === RemoteDataState.NotStarted) {
      return (
        <EmptyGraphMessage
          message={emptyGraphCopy}
          testID="empty-graph--no-queries"
        />
      )
    }

    if (status === RemoteDataState.Loading) {
      return <EmptyGraphMessage message="Loading..." />
    }

    if (error) {
        return (
          <EmptyGraphError message={error} testID="empty-graph--error" />
        )
    }

    if (status === RemoteDataState.Done && !hasResults) {
      return (
        <EmptyGraphMessage
          message="No Results"
          testID="empty-graph--no-results"
        />
      )
    }

    return null
}

export default EmptyQuery
