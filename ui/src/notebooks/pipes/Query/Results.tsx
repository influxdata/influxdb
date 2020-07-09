// Libraries
import React, {FC, useEffect, useState, useContext, useMemo} from 'react'
import {AutoSizer} from 'react-virtualized'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import {ROW_HEIGHT} from 'src/timeMachine/components/RawFluxDataGrid'
import Resizer from 'src/notebooks/shared/Resizer'
import ResultsPagination from 'src/notebooks/pipes/Query/ResultsPagination'

import {PipeContext} from 'src/notebooks/context/pipe'
import {RemoteDataState} from 'src/types'

// Utils
import {event} from 'src/notebooks/shared/event'

const Results: FC = () => {
  const {data, results, loading} = useContext(PipeContext)
  const resultsExist =
    !!results && !!results.raw && !!results.parsed.table.length
  const raw = (results || {}).raw || ''

  const rows = useMemo(() => raw.split('\n'), [raw])
  const [startRow, setStartRow] = useState<number>(0)
  const [pageSize, setPageSize] = useState<number>(0)

  useEffect(() => {
    setStartRow(0)
  }, [raw])

  const prevDisabled = startRow <= 0
  const nextDisabled = startRow + pageSize >= rows.length

  const prev = () => {
    event('Query Pagination Previous Button Clicked')

    const index = startRow - pageSize
    if (index <= 0) {
      setStartRow(0)
      return
    }
    setStartRow(index)
  }

  const next = () => {
    event('Query Pagination Next Button Clicked')

    const index = startRow + pageSize
    const max = rows.length - pageSize
    if (index >= max) {
      setStartRow(max)
      return
    }
    setStartRow(index)
  }

  let emptyText
  if (loading === RemoteDataState.NotStarted) {
    emptyText = 'Run the Flow to See Results'
  } else if (loading === RemoteDataState.Loading) {
    emptyText = 'Loading'
  } else {
    emptyText = 'No Data Returned'
  }

  return (
    <Resizer
      resizingEnabled={resultsExist}
      emptyText={emptyText}
      error={results.error}
      hiddenText="Results hidden"
      toggleVisibilityEnabled={true}
    >
      <div className="query-results">
        <ResultsPagination
          onClickPrev={prev}
          onClickNext={next}
          disablePrev={prevDisabled}
          disableNext={nextDisabled}
          visible={resultsExist && data.panelVisibility === 'visible'}
          pageSize={pageSize}
          startRow={startRow}
        />
        <div className="query-results--container">
          <AutoSizer>
            {({width, height}) => {
              if (!width || !height) {
                return false
              }

              const page = Math.floor(height / ROW_HEIGHT)
              setPageSize(page)

              return (
                <RawFluxDataTable
                  files={[rows.slice(startRow, startRow + page).join('\n')]}
                  width={width}
                  height={page * ROW_HEIGHT}
                  disableVerticalScrolling={true}
                />
              )
            }}
          </AutoSizer>
        </div>
      </div>
    </Resizer>
  )
}

export default Results
