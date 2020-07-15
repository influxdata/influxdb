// Libraries
import React, {useLayoutEffect, FC, useEffect, useState} from 'react'
import {useDispatch} from 'react-redux'
import {AutoSizer, InfiniteLoader, List} from 'react-virtualized'

// Components
import Header from 'src/eventViewer/components/Header'
import TableRow from 'src/eventViewer/components/TableRow'
import LoadingRow from 'src/eventViewer/components/LoadingRow'
import FooterRow from 'src/eventViewer/components/FooterRow'
import ErrorRow from 'src/eventViewer/components/ErrorRow'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Utils
import {
  loadNextRows,
  getRowCount,
} from 'src/eventViewer/components/EventViewer.reducer'

// Types
import {EventViewerChildProps, Fields} from 'src/eventViewer/types'
import {RemoteDataState} from 'src/types'

// Constants
import {checkStatusLoading} from 'src/shared/copy/notifications'

type OwnProps = {
  fields: Fields
}

type Props = EventViewerChildProps & OwnProps

const rowLoadedFn = state => ({index}) => !!state.rows[index]

const EventTable: FC<Props> = ({state, dispatch, loadRows, fields}) => {
  const rowCount = getRowCount(state)

  const isRowLoaded = rowLoadedFn(state)

  const isRowLoadedBoolean = !!state.rows[0]

  const loadMoreRows = () => loadNextRows(state, dispatch, loadRows)

  const [isLongRunningQuery, setIsLongRunningQuery] = useState(false)

  const reduxDispatch = useDispatch()

  useEffect(() => {
    const timeoutID = setTimeout(() => {
      setIsLongRunningQuery(true)
    }, 5000)
    return () => clearTimeout(timeoutID)
  }, [setIsLongRunningQuery])

  useEffect(() => {
    if (isLongRunningQuery && !isRowLoadedBoolean) {
      reduxDispatch(notify(checkStatusLoading))
    }
  }, [isLongRunningQuery, isRowLoadedBoolean, reduxDispatch])

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

export default EventTable
