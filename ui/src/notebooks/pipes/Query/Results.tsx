// Libraries
import React, {FC} from 'react'
import {BothResults} from 'src/notebooks/context/query'
import {AutoSizer} from 'react-virtualized'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import ResultsResizer from 'src/notebooks/pipes/Query/ResultsResizer'

// Types
import {ResultsVisibility} from 'src/notebooks/pipes/Query'

interface Props {
  results: BothResults
  visibility: ResultsVisibility
  onUpdate: (data: any) => void
  height: number
}

const Results: FC<Props> = ({results, visibility, onUpdate, height}) => {
  const resultsExist = !!results.raw

  const handleUpdateResultsVisibility = (
    resultsVisibility: ResultsVisibility
  ): void => {
    onUpdate({resultsVisibility})
  }

  const handleUpdateResultsHeight = (resultsPanelHeight: number): void => {
    onUpdate({resultsPanelHeight})
  }

  let resultsBody = (
    <div className="notebook-raw-data--empty">Run the Flow to see results</div>
  )

  if (resultsExist && visibility === 'hidden') {
    resultsBody = <div className="notebook-raw-data--empty">Results hidden</div>
  }

  if (resultsExist && visibility === 'visible') {
    resultsBody = (
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
    )
  }

  return (
    <div className="notebook-raw-data">
      <ResultsResizer
        visibility={visibility}
        height={height}
        onUpdateHeight={handleUpdateResultsHeight}
        onUpdateVisibility={handleUpdateResultsVisibility}
        resizingEnabled={resultsExist}
      >
        {resultsBody}
      </ResultsResizer>
    </div>
  )
}

export default Results
