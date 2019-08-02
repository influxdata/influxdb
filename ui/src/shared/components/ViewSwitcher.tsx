// Libraries
import React, {FunctionComponent} from 'react'
import {Plot, FromFluxResult} from '@influxdata/giraffe'

// Components
import GaugeChart from 'src/shared/components/GaugeChart'
import SingleStat from 'src/shared/components/SingleStat'
import TableGraphs from 'src/shared/components/tables/TableGraphs'
import HistogramPlot from 'src/shared/components/HistogramPlot'
import HeatmapPlot from 'src/shared/components/HeatmapPlot'
import FluxTablesTransform from 'src/shared/components/FluxTablesTransform'
import XYPlot from 'src/shared/components/XYPlot'
import ScatterPlot from 'src/shared/components/ScatterPlot'
import LatestValueTransform from 'src/shared/components/LatestValueTransform'

// Types
import {
  QueryViewProperties,
  SingleStatViewProperties,
  XYViewProperties,
  RemoteDataState,
  TimeZone,
  CheckViewProperties,
} from 'src/types'

interface Props {
  giraffeResult: FromFluxResult
  files: string[]
  loading: RemoteDataState
  properties: QueryViewProperties | CheckViewProperties
  timeZone: TimeZone
}

const ViewSwitcher: FunctionComponent<Props> = ({
  properties,
  loading,
  files,
  giraffeResult: {table, fluxGroupKeyUnion},
  timeZone,
}) => {
  switch (properties.type) {
    case 'single-stat':
      return (
        <LatestValueTransform table={table}>
          {latestValue => (
            <SingleStat stat={latestValue} properties={properties} />
          )}
        </LatestValueTransform>
      )

    case 'table':
      return (
        <FluxTablesTransform files={files}>
          {tables => (
            <TableGraphs
              tables={tables}
              properties={properties}
              timeZone={timeZone}
            />
          )}
        </FluxTablesTransform>
      )

    case 'gauge':
      return (
        <LatestValueTransform table={table}>
          {latestValue => (
            <GaugeChart value={latestValue} properties={properties} />
          )}
        </LatestValueTransform>
      )
    case 'check':
    case 'xy':
      return (
        <XYPlot
          table={table}
          fluxGroupKeyUnion={fluxGroupKeyUnion}
          viewProperties={properties}
          loading={loading}
          timeZone={timeZone}
        >
          {config => <Plot config={config} />}
        </XYPlot>
      )

    case 'line-plus-single-stat':
      const xyProperties = {
        ...properties,
        colors: properties.colors.filter(c => c.type === 'scale'),
        type: 'xy' as 'xy',
        geom: 'line' as 'line',
      } as XYViewProperties

      const singleStatProperties = {
        ...properties,
        colors: properties.colors.filter(c => c.type !== 'scale'),
        type: 'single-stat',
      } as SingleStatViewProperties

      return (
        <XYPlot
          table={table}
          fluxGroupKeyUnion={fluxGroupKeyUnion}
          viewProperties={xyProperties}
          loading={loading}
          timeZone={timeZone}
        >
          {config => (
            <Plot config={config}>
              <LatestValueTransform table={config.table} quiet={true}>
                {latestValue => (
                  <SingleStat
                    stat={latestValue}
                    properties={singleStatProperties}
                  />
                )}
              </LatestValueTransform>
            </Plot>
          )}
        </XYPlot>
      )

    case 'histogram':
      return (
        <HistogramPlot
          table={table}
          loading={loading}
          timeZone={timeZone}
          viewProperties={properties}
        >
          {config => <Plot config={config} />}
        </HistogramPlot>
      )

    case 'heatmap':
      return (
        <HeatmapPlot
          table={table}
          loading={loading}
          timeZone={timeZone}
          viewProperties={properties}
        >
          {config => <Plot config={config} />}
        </HeatmapPlot>
      )

    case 'scatter':
      return (
        <ScatterPlot
          table={table}
          loading={loading}
          viewProperties={properties}
          timeZone={timeZone}
        >
          {config => <Plot config={config} />}
        </ScatterPlot>
      )

    default:
      return <div />
  }
}

export default ViewSwitcher
