// Libraries
import React, {FC, useEffect, useState, useMemo} from 'react'
import {BothResults} from 'src/notebooks/context/query'
import {AutoSizer} from 'react-virtualized'
import classnames from 'classnames'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import {ROW_HEIGHT} from 'src/timeMachine/components/RawFluxDataGrid'
import Resizer from 'src/notebooks/shared/Resizer'

// Types
import {PipeData} from 'src/notebooks/index'

interface Props {
  data: PipeData
  results: BothResults
  onUpdate: (data: any) => void
}

const Results: FC<Props> = ({results, onUpdate, data}) => {
  const resultsExist = !!results.raw && !!results.parsed.table.length

  const rows = useMemo(() => (results.raw || '').split('\n'), [results.raw])
  const [startRow, setStartRow] = useState(0)
  const [pageSize, setPageSize] = useState(0)

  useEffect(() => {
    setStartRow(0)
  }, [results.raw])

  const prevClass = classnames('notebook-raw-data--prev', {
    'notebook-raw-data--button__disabled': startRow <= 0,
  })
  const nextClass = classnames('notebook-raw-data--next', {
    'notebook-raw-data--button__disabled': startRow + pageSize >= rows.length,
  })
  const prev = () => {
    const index = startRow - pageSize
    if (index <= 0) {
      setStartRow(0)
      return
    }
    setStartRow(index)
  }

  const next = () => {
    const index = startRow + pageSize
    const max = rows.length - pageSize
    if (index >= max) {
      setStartRow(max)
      return
    }
    setStartRow(index)
  }

  return (
    <>
      <Resizer
        data={data}
        onUpdate={onUpdate}
        resizingEnabled={resultsExist}
        emptyText="Run the Flow to see Results"
        hiddenText="Results hidden"
        toggleVisibilityEnabled={true}
      >
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
              />
            )
          }}
        </AutoSizer>
      </Resizer>
      {resultsExist && data.resultsVisibility === 'visible' && (
        <div>
          <div className={prevClass} onClick={prev}>
            prev
          </div>
          <div className={nextClass} onClick={next}>
            next
          </div>
        </div>
      )}
    </>
  )
}

export default Results
