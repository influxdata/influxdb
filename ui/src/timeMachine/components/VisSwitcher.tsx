// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {AutoSizer} from 'react-virtualized'
import {Plot} from '@influxdata/vis'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import HistogramContainer from 'src/shared/components/HistogramContainer'
import HistogramTransform from 'src/timeMachine/components/HistogramTransform'
import RefreshingViewSwitcher from 'src/shared/components/RefreshingViewSwitcher'

// Utils
import {getActiveTimeMachine, getTables} from 'src/timeMachine/selectors'

// Types
import {
  ViewType,
  QueryViewProperties,
  FluxTable,
  RemoteDataState,
  AppState,
} from 'src/types'

interface StateProps {
  files: string[]
  tables: FluxTable[]
  loading: RemoteDataState
  properties: QueryViewProperties
  isViewingRawData: boolean
}

const VisSwitcher: FunctionComponent<StateProps> = ({
  files,
  tables,
  loading,
  properties,
  isViewingRawData,
}) => {
  if (isViewingRawData) {
    return (
      <AutoSizer>
        {({width, height}) =>
          width &&
          height && (
            <RawFluxDataTable files={files} width={width} height={height} />
          )
        }
      </AutoSizer>
    )
  }

  if (properties.type === ViewType.Histogram) {
    // Histograms have special treatment when rendered within a time machine:
    // if the backing view for the histogram has `xColumn` and `fillColumn`
    // selections that are invalid given the current query response, then we
    // fall back to using valid selections for those fields (if available).
    // When a histogram is rendered on a dashboard, we use the selections
    // stored in the view verbatim and display an error if they are invalid.
    return (
      <HistogramTransform>
        {({table, xColumn, fillColumns}) => (
          <HistogramContainer
            table={table}
            loading={loading}
            viewProperties={{...properties, xColumn, fillColumns}}
          >
            {config => <Plot config={config} />}
          </HistogramContainer>
        )}
      </HistogramTransform>
    )
  }

  return (
    <RefreshingViewSwitcher
      tables={tables}
      files={files}
      loading={loading}
      properties={properties}
    />
  )
}

const mstp = (state: AppState) => {
  const {
    view: {properties},
    isViewingRawData,
    queryResults: {status: loading, files},
  } = getActiveTimeMachine(state)

  const tables = getTables(state)

  return {
    files,
    tables,
    loading,
    properties,
    isViewingRawData,
  }
}

export default connect<StateProps>(mstp)(VisSwitcher)
