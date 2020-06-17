// Libraries
import React, {FC, useEffect, useState, useMemo} from 'react'
import {BothResults} from 'src/notebooks'
import {AutoSizer} from 'react-virtualized'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import {ROW_HEIGHT} from 'src/timeMachine/components/RawFluxDataGrid'
import Resizer from 'src/notebooks/shared/Resizer'
import ResultsPagination from 'src/notebooks/pipes/Query/ResultsPagination'

// Types
import {PipeData} from 'src/notebooks/index'

// Utils
import {event} from 'src/notebooks/shared/event'

interface Props {
  data: PipeData
  results: BothResults
  onUpdate: (data: any) => void
}

const Results: FC<Props> = ({results, onUpdate, data}) => {
  const resultsExist = !!results.raw && !!results.parsed.table.length

  const rows = useMemo(() => (results.raw || '').split('\n'), [results.raw])
  const [startRow, setStartRow] = useState<number>(0)
  const [pageSize, setPageSize] = useState<number>(0)

  useEffect(() => {
    setStartRow(0)
  }, [results.raw])

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

  return (
    <Resizer
      data={data}
      onUpdate={onUpdate}
      resizingEnabled={resultsExist}
      emptyText="Run the Flow to see Results"
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
