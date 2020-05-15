// Libraries
import React, {useLayoutEffect, FC, useEffect, useState} from 'react'
import {AutoSizer, InfiniteLoader, List} from 'react-virtualized'

// Components
import Header from 'src/eventViewer/components/Header'
import TableRow from 'src/eventViewer/components/TableRow'
import LoadingRow from 'src/eventViewer/components/LoadingRow'
import FooterRow from 'src/eventViewer/components/FooterRow'
import ErrorRow from 'src/eventViewer/components/ErrorRow'
import {Notification, Gradients, IconFont, ComponentSize} from '@influxdata/clockface'

// Utils
import {
  loadNextRows,
  getRowCount,
} from 'src/eventViewer/components/EventViewer.reducer'

// Types
import {EventViewerChildProps, Fields} from 'src/eventViewer/types'
import {RemoteDataState} from 'src/types'

type Props = EventViewerChildProps & {
  fields: Fields
}

const EventTable: FC<Props> = ({state, dispatch, loadRows, fields}) => {
  const rowCount = getRowCount(state)

  const isRowLoaded = ({index}) => !!state.rows[index]

  const loadMoreRows = () => loadNextRows(state, dispatch, loadRows)

  const [isLongRunningQuery, setIsLongRunningQuery] = useState(false)

  useEffect(() => {
    console.log("useeffectbeingcalled")
    setTimeout(
    ()=>{setIsLongRunningQuery(true)}, 5000 
    )
  })

  useEffect(() => {
    console.log("somethingchanged")
    if (isLongRunningQuery && !isRowLoaded) {
      //notify
      alert("notificationcomingsoon")
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

export default EventTable
