// Libraries
import React, {useLayoutEffect, FC, useEffect, useState} from 'react'
import {connect} from 'react-redux'
import {AutoSizer, InfiniteLoader, List} from 'react-virtualized'

// Components
import Header from 'src/eventViewer/components/Header'
import TableRow from 'src/eventViewer/components/TableRow'
import LoadingRow from 'src/eventViewer/components/LoadingRow'
import FooterRow from 'src/eventViewer/components/FooterRow'
import ErrorRow from 'src/eventViewer/components/ErrorRow'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Utils
import {
  loadNextRows,
  getRowCount,
} from 'src/eventViewer/components/EventViewer.reducer'

// Types
import {EventViewerChildProps, Fields} from 'src/eventViewer/types'
import {RemoteDataState} from 'src/types'

// Constants
import * as copy from 'src/shared/copy/notifications'

type DispatchProps = {
  notify: typeof notifyAction
}

type OwnProps = {
  fields: Fields
}

type Props = EventViewerChildProps & DispatchProps & OwnProps

const EventTable: FC<Props> = ({state, dispatch, loadRows, fields, notify}) => {
  const rowCount = getRowCount(state)

  const isRowLoaded = ({index}) => !!state.rows[index]

  const isRowLoadedBoolean = !!state.rows[0]

  const loadMoreRows = () => loadNextRows(state, dispatch, loadRows)

  const [isLongRunningQuery, setIsLongRunningQuery] = useState(false)

  useEffect(() => {
    setTimeout(() => {
      setIsLongRunningQuery(true)
    }, 5000)
  })

  useEffect(() => {
    if (isLongRunningQuery && !isRowLoadedBoolean) {
      notify(copy.checkStatusLoading)
    }
  }, [isLongRunningQuery, isRowLoaded])

  const rowRenderer = ({key, index, style}) => {
    const isLastRow = index === state.rows.length

    if (isLastRow && state.nextRowsStatus === RemoteDataState.Error) {
      return <ErrorRow key={key} index={index} style={style} />
    }

    if (isLastRow && state.hasReachedEnd) {
      return <FooterRow key={key} style={style} />
    }

    if (!state.rows[index]) {
      return <LoadingRow key={key} index={index} style={style} />
    }

    return (
      <TableRow
        key={key}
        style={style}
        row={state.rows[index]}
        fields={fields}
      />
    )
  }

  const scrollTop =
    state.nextScrollTop === null ? undefined : state.nextScrollTop

  useLayoutEffect(() => {
    dispatch({type: 'CONSUMED_NEXT_SCROLL_INDEX'})
  }, [scrollTop, dispatch])

  return (
    <div className="event-table">
      <Header fields={fields} />
      <div className="event-table--table">
        <AutoSizer>
          {({width, height}) => {
            if (!width || !height) {
              return null
            }

            return (
              <InfiniteLoader
                isRowLoaded={isRowLoaded}
                loadMoreRows={loadMoreRows}
                rowCount={rowCount}
              >
                {({onRowsRendered, registerChild}) => (
                  <List
                    width={width}
                    height={height}
                    ref={registerChild}
                    onRowsRendered={onRowsRendered}
                    rowCount={rowCount}
                    rowHeight={38}
                    rowRenderer={rowRenderer}
                    overscanRowCount={20}
                    scrollTop={scrollTop}
                    onScroll={({scrollTop}) =>
                      dispatch({type: 'SCROLLED', scrollTop})
                    }
                  />
                )}
              </InfiniteLoader>
            )
          }}
        </AutoSizer>
      </div>
    </div>
  )
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(EventTable)
