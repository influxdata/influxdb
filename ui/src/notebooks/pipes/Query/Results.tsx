// Libraries
import React, {FC} from 'react'
import {BothResults} from 'src/notebooks/context/query'
import {AutoSizer} from 'react-virtualized'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import ResultsResizer from 'src/notebooks/pipes/Query/ResultsResizer'

interface Props {
  data: any
  results: BothResults
  onUpdate: (data: any) => void
}

const Results: FC<Props> = ({results, onUpdate, data}) => {
  const resultsExist = !!results.raw

  return (
    <div className="notebook-raw-data">
      <ResultsResizer
        data={data}
        onUpdate={onUpdate}
        resizingEnabled={resultsExist}
      >
        <AutoSizer>
          {({width, height}) =>
            width &&
            height && (
              <RawFluxDataTable
                files={[results.raw]}
                width={width}
                height={height}
              />
            )
          }
        </AutoSizer>
      </ResultsResizer>
    </div>
  )
}

export default Results
