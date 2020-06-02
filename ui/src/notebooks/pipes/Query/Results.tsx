// Libraries
import React, {FC} from 'react'
import {BothResults} from 'src/notebooks/context/query'
import {AutoSizer} from 'react-virtualized'
import classnames from 'classnames'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import ResultsHeader from 'src/notebooks/pipes/Query/ResultsHeader'

// Types
import {RawDataSize} from 'src/notebooks/pipes/Query'

interface Props {
  results: BothResults
  size: RawDataSize
  onUpdateSize: (size: RawDataSize) => void
}

const Results: FC<Props> = ({results, size, onUpdateSize}) => {
  const resultsExist = !!results.raw
  const className = classnames('notebook-raw-data', {
    [`notebook-raw-data__${size}`]: resultsExist && size,
  })

  let resultsBody = (
    <div className="notebook-raw-data--empty">
      Run the Notebook to see results
    </div>
  )

  if (resultsExist) {
    resultsBody = (
      <div className="notebook-raw-data--body">
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
      </div>
    )
  }

  return (
    <div className={className}>
      <ResultsHeader
        resultsExist={resultsExist}
        size={size}
        onUpdateSize={onUpdateSize}
      />
      {resultsBody}
    </div>
  )
}

export default Results
