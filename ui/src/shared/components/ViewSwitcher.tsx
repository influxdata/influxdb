// Libraries
import React, {FunctionComponent} from 'react'
import {Plot, FromFluxResult} from '@influxdata/giraffe'

// Components
import GaugeChart from 'src/shared/components/GaugeChart'
import SingleStat from 'src/shared/components/SingleStat'
import TableGraphs from 'src/shared/components/tables/TableGraphs'
import HistogramContainer from 'src/shared/components/HistogramContainer'
import HeatmapContainer from 'src/shared/components/HeatmapContainer'
import FluxTablesTransform from 'src/shared/components/FluxTablesTransform'
import XYContainer from 'src/shared/components/XYContainer'
import ScatterContainer from 'src/shared/components/ScatterContainer'
import LatestValueTransform from 'src/shared/components/LatestValueTransform'

// Types
import {
  QueryViewProperties,
  ViewType,
  SingleStatView,
  XYView,
  XYViewGeom,
  RemoteDataState,
  TimeZone,
} from 'src/types'

interface Props {
  giraffeResult: FromFluxResult
  files: string[]
  loading: RemoteDataState
  properties: QueryViewProperties
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
    case ViewType.SingleStat:
      return (
        <LatestValueTransform table={table}>
          {latestValue => (
            <SingleStat stat={latestValue} properties={properties} />
          )}
        </LatestValueTransform>
      )

    case ViewType.Table:
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

    case ViewType.Gauge:
      return (
        <LatestValueTransform table={table}>
          {latestValue => (
            <GaugeChart value={latestValue} properties={properties} />
          )}
        </LatestValueTransform>
      )

    case ViewType.XY:
      return (
        <XYContainer
          table={table}
          fluxGroupKeyUnion={fluxGroupKeyUnion}
          viewProperties={properties}
          loading={loading}
          timeZone={timeZone}
        >
          {config => <Plot config={config} />}
        </XYContainer>
      )

    case ViewType.LinePlusSingleStat:
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
        <XYContainer
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
        </XYContainer>
      )

    case ViewType.Histogram:
      return (
        <HistogramContainer
          table={table}
          loading={loading}
          timeZone={timeZone}
          viewProperties={properties}
        >
          {config => <Plot config={config} />}
        </HistogramContainer>
      )

    case ViewType.Heatmap:
      return (
        <HeatmapContainer
          table={table}
          loading={loading}
          timeZone={timeZone}
          viewProperties={properties}
        >
          {config => <Plot config={config} />}
        </HeatmapContainer>
      )

    case ViewType.Scatter:
      return (
        <ScatterContainer
          table={table}
          loading={loading}
          viewProperties={properties}
          timeZone={timeZone}
        >
          {config => <Plot config={config} />}
        </ScatterContainer>
      )

    default:
      return <div />
  }
}

export default ViewSwitcher
