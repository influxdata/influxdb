// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {AutoSizer} from 'react-virtualized'
import {Plot} from '@influxdata/giraffe'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import HistogramContainer from 'src/shared/components/HistogramContainer'
import VisDataTransform from 'src/timeMachine/components/VisDataTransform'
import RefreshingViewSwitcher from 'src/shared/components/RefreshingViewSwitcher'
import HeatmapContainer from 'src/shared/components/HeatmapContainer'
import ScatterContainer from 'src/shared/components/ScatterContainer'
import SingleStat from 'src/shared/components/SingleStat'
import LatestValueTransform from 'src/shared/components/LatestValueTransform'

// Utils
import {getActiveTimeMachine, getTables} from 'src/timeMachine/selectors'

// Types
import {
  ViewType,
  QueryViewProperties,
  FluxTable,
  RemoteDataState,
  AppState,
  XYViewGeom,
  XYView,
  SingleStatView,
} from 'src/types'
import XYContainer from 'src/shared/components/XYContainer'

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

  // Histograms and heatmaps have special treatment when rendered within a time
  // machine, since they allow for selecting which query response columns are
  // visualized.  If the column selections are invalid given the current query
  // response, then we fall back to using valid selections for those fields if
  // possible.  This is in contrast to when these visualizations are rendered
  // on a dashboard; in this case we use the selections stored in the view
  // verbatim and display an error if they are invalid.
  if (properties.type === ViewType.Histogram) {
    return (
      <VisDataTransform>
        {({table, xColumn, fillColumns}) => (
          <HistogramContainer
            table={table}
            loading={loading}
            viewProperties={{...properties, xColumn, fillColumns}}
          >
            {config => <Plot config={config} />}
          </HistogramContainer>
        )}
      </VisDataTransform>
    )
  }

  if (properties.type === ViewType.Heatmap) {
    return (
      <VisDataTransform>
        {({table, xColumn, yColumn}) => (
          <HeatmapContainer
            table={table}
            loading={loading}
            viewProperties={{...properties, xColumn, yColumn}}
          >
            {config => <Plot config={config} />}
          </HeatmapContainer>
        )}
      </VisDataTransform>
    )
  }

  if (properties.type === ViewType.Scatter) {
    return (
      <VisDataTransform>
        {({table, xColumn, yColumn, fillColumns, symbolColumns}) => (
          <ScatterContainer
            table={table}
            loading={loading}
            viewProperties={{
              ...properties,
              xColumn,
              yColumn,
              fillColumns,
              symbolColumns,
            }}
          >
            {config => <Plot config={config} />}
          </ScatterContainer>
        )}
      </VisDataTransform>
    )
  }

  if (properties.type === ViewType.XY) {
    return (
      <VisDataTransform>
        {({table, fluxGroupKeyUnion, xColumn, yColumn}) => (
          <XYContainer
            table={table}
            fluxGroupKeyUnion={fluxGroupKeyUnion}
            loading={loading}
            viewProperties={{
              ...properties,
              xColumn,
              yColumn,
            }}
          >
            {config => <Plot config={config} />}
          </XYContainer>
        )}
      </VisDataTransform>
    )
  }

  if (properties.type === ViewType.LinePlusSingleStat) {
    const xyProperties = {
      ...properties,
      colors: properties.colors.filter(c => c.type === 'scale'),
      type: ViewType.XY,
      geom: XYViewGeom.Line,
    } as XYView

    const singleStatProperties = {
      ...properties,
      colors: properties.colors.filter(c => c.type !== 'scale'),
      type: ViewType.SingleStat,
    } as SingleStatView

    return (
      <VisDataTransform>
        {({table, fluxGroupKeyUnion, xColumn, yColumn}) => (
          <XYContainer
            table={table}
            fluxGroupKeyUnion={fluxGroupKeyUnion}
            loading={loading}
            viewProperties={{
              ...xyProperties,
              xColumn,
              yColumn,
            }}
          >
            {config => (
              <Plot config={config}>
                <LatestValueTransform table={table} quiet={true}>
                  {latestValue => (
                    <SingleStat
                      stat={latestValue}
                      properties={singleStatProperties}
                    />
                  )}
                </LatestValueTransform>
              </Plot>
            )}
          </XYContainer>
        )}
      </VisDataTransform>
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
