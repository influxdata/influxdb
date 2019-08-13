// Libraries
import React, {useState, FC} from 'react'
import {Input, IconFont} from '@influxdata/clockface'
import {isEqual} from 'lodash'

// Actions
import {
  search,
  clearSearch,
} from 'src/eventViewer/components/EventViewer.reducer'

// Utils
import {useDebouncedValue} from 'src/shared/utils/useDebouncedValue'
import {useMountedEffect} from 'src/shared/utils/useMountedEffect'

// Types
import {EventViewerChildProps, SearchExpr} from 'src/eventViewer/types'

const SEARCH_DELAY_MS = 500

// Given a search term, build a search expression representing:
//
//     checkName =~ /term/ or message =~ /term/
//
// An empty search term results in a null expression.
const searchExprForTerm = (term: string): SearchExpr | null => {
  if (term.trim() === '') {
    return null
  }

  return {
    type: 'BINARY_EXPR',
    op: 'OR',
    left: {
      type: 'BINARY_EXPR',
      op: 'REG_MATCH',
      left: 'checkName',
      right: term,
    },
    right: {
      type: 'BINARY_EXPR',
      op: 'REG_MATCH',
      left: 'message',
      right: term,
    },
  }
}

type Props = EventViewerChildProps & {
  placeholder?: string
}

const StatusSearchBar: FC<Props> = ({state, dispatch, loadRows}) => {
  const [searchTerm, setSearchTerm] = useState<string>('')
  const debouncedSearchTerm = useDebouncedValue(searchTerm, SEARCH_DELAY_MS)
  const searchExpr = searchExprForTerm(debouncedSearchTerm)

  useMountedEffect(() => {
    if (searchExpr && !isEqual(searchExpr, state.searchExpr)) {
      search(state, dispatch, loadRows, searchExpr)
    } else if (!isEqual(searchExpr, state.searchExpr)) {
      clearSearch(state, dispatch, loadRows)
    }
  }, [state, dispatch, loadRows, searchExpr])

  return (
    <Input
      icon={IconFont.Search}
      className="status-search-bar"
      placeholder='e.g. "crit" or "my check"'
      value={searchTerm}
      onChange={e => setSearchTerm(e.target.value)}
    />
  )
}

export default StatusSearchBar
