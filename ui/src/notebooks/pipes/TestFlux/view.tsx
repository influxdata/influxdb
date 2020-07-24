// Libraries
import React, {FC, useState, useContext} from 'react'

// Components
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import {ViewTypeDropdown} from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import Resizer from 'src/notebooks/shared/Resizer'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'

// Utilities
import fromFlux from 'src/shared/utils/fromFlux.legacy'
import {checkResultsLength} from 'src/shared/utils/vis'

// Types
import {PipeProp, FluxResult} from 'src/notebooks'
import {ViewType, RemoteDataState} from 'src/types'

import {AppSettingContext} from 'src/notebooks/context/app'
import {PipeContext} from 'src/notebooks/context/pipe'

import {updateVisualizationType} from 'src/notebooks/pipes/Visualization/view'

const TestFlux: FC<PipeProp> = ({Context}) => {
  const {timeZone} = useContext(AppSettingContext)
  const {data, update} = useContext(PipeContext)
  const uploadRef: React.RefObject<HTMLInputElement> = React.createRef()
  const startUpload = () => {
    uploadRef.current.click()
  }
  const parseCSV = evt => {
    Promise.all(
      Array.from(evt.target.files)
        .filter((file: File) => file.type === 'text/csv')
        .map((file: File) => {
          return new Promise((resolve, reject) => {
            const reader = new FileReader()
            reader.onload = () => {
              resolve(reader.result)
            }
            reader.onerror = () => {
              reject()
            }
            reader.readAsText(file)
          })
        })
    )
      .then(results => {
        const result = results.join('\n\n')

        return {
          raw: result,
          parsed: fromFlux(result),
          source: 'buckets()',
        } as FluxResult
      })
      .then(result => {
        setResults(result)
      })
  }
  const [results, setResults] = useState({} as FluxResult)

  const updateType = (type: ViewType) => {
    updateVisualizationType(type, results.parsed, update)
  }

  const controls = (
    <>
      <ViewTypeDropdown
        viewType={data.properties.type}
        onUpdateType={updateType as any}
      />
      <SquareButton
        icon={IconFont.Import}
        titleText="Import CSV"
        onClick={startUpload}
      />
      <input type="file" ref={uploadRef} onChange={parseCSV} hidden />
    </>
  )

  return (
    <Context controls={controls}>
      <Resizer
        resizingEnabled={!!results.raw}
        emptyText="This cell will visualize results from uploaded CSVs"
        emptyIcon={IconFont.BarChart}
        toggleVisibilityEnabled={false}
      >
        <div className="notebook-visualization">
          <div className="notebook-visualization--view">
            <EmptyQueryView
              loading={RemoteDataState.Done}
              errorMessage={results.error}
              errorFormat={ErrorFormat.Scroll}
              hasResults={checkResultsLength(results.parsed)}
            >
              <ViewSwitcher
                giraffeResult={results.parsed}
                files={[results.raw]}
                properties={data.properties}
                timeZone={timeZone}
                theme="dark"
              />
            </EmptyQueryView>
          </div>
        </div>
      </Resizer>
    </Context>
  )
}

export default TestFlux
