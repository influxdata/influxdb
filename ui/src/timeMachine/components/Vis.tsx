// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {FromFluxResult} from '@influxdata/giraffe'
import {AutoSizer} from 'react-virtualized'

// Components
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {checkResultsLength} from 'src/shared/utils/vis'
import {
  getVisTable,
  getXColumnSelection,
  getYColumnSelection,
  getFillColumnsSelection,
  getSymbolColumnsSelection,
} from 'src/timeMachine/selectors'

// Types
import {RemoteDataState, AppState, QueryViewProperties} from 'src/types'

interface StateProps {
  loading: RemoteDataState
  errorMessage: string
  files: string[]
  viewProperties: QueryViewProperties
  isInitialFetch: boolean
  isViewingRawData: boolean
  giraffeResult: FromFluxResult
  xColumn: string
  yColumn: string
  fillColumns: string[]
  symbolColumns: string[]
}

type Props = StateProps

const TimeMachineVis: SFC<Props> = ({
  loading,
  errorMessage,
  isInitialFetch,
  isViewingRawData,
  files,
  viewProperties,
  giraffeResult,
  xColumn,
  yColumn,
  fillColumns,
  symbolColumns,
}) => {
  // If the current selections for `xColumn`/`yColumn`/ etc. are invalid given
  // the current Flux response, attempt to make a valid selection instead. This
  // fallback logic is contained within the selectors that supply each of these
  // props. Note that in a dashboard context, we display an error instead of
  // attempting to fall back to an valid selection.
  const resolvedViewProperties = {
    ...viewProperties,
    xColumn,
    yColumn,
    fillColumns,
    symbolColumns,
  }

  return (
    <div className="time-machine--view">
      <EmptyQueryView
        loading={loading}
        errorFormat={ErrorFormat.Scroll}
        errorMessage={errorMessage}
        isInitialFetch={isInitialFetch}
        queries={viewProperties.queries}
        hasResults={checkResultsLength(giraffeResult)}
      >
        {isViewingRawData ? (
          <AutoSizer>
            {({width, height}) =>
              width &&
              height && (
                <RawFluxDataTable files={files} width={width} height={height} />
              )
            }
          </AutoSizer>
        ) : (
          <ViewSwitcher
            giraffeResult={giraffeResult}
            files={files}
            loading={loading}
            properties={resolvedViewProperties}
          />
        )}
      </EmptyQueryView>
    </div>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    isViewingRawData,
    view: {properties: viewProperties},
    queryResults: {status: loading, errorMessage, isInitialFetch, files},
  } = getActiveTimeMachine(state)

  const giraffeResult = getVisTable(state)
  const xColumn = getXColumnSelection(state)
  const yColumn = getYColumnSelection(state)
  const fillColumns = getFillColumnsSelection(state)
  const symbolColumns = getSymbolColumnsSelection(state)

  return {
    loading,
    errorMessage,
    isInitialFetch,
    files,
    viewProperties,
    isViewingRawData,
    giraffeResult,
    xColumn,
    yColumn,
    fillColumns,
    symbolColumns,
  }
}

export default connect<StateProps>(mstp)(TimeMachineVis)
