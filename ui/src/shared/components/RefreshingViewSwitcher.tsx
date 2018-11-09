// Libraries
import React, {PureComponent} from 'react'

// Components
import LineGraph from 'src/shared/components/LineGraph'
import StepPlot from 'src/shared/components/StepPlot'
import Stacked from 'src/shared/components/Stacked'
import GaugeChart from 'src/shared/components/GaugeChart'
import SingleStat from 'src/shared/components/SingleStat'
import SingleStatTransform from 'src/shared/components/SingleStatTransform'
import TimeMachineTables from 'src/shared/components/tables/TimeMachineTables'

// Types
import {
  RefreshingViewProperties,
  ViewType,
  LineView,
  SingleStatView,
} from 'src/types/v2/dashboards'
import {FluxTable, RemoteDataState, TimeRange} from 'src/types'

interface Props {
  viewID: string
  tables: FluxTable[]
  loading: RemoteDataState
  timeRange: TimeRange
  onZoom: (range: TimeRange) => void
  properties: RefreshingViewProperties
}

export default class RefreshingViewSwitcher extends PureComponent<Props> {
  public render() {
    const {properties, loading, viewID, tables, onZoom, timeRange} = this.props

    switch (properties.type) {
      case ViewType.SingleStat:
        return (
          <SingleStatTransform tables={tables}>
            {stat => <SingleStat stat={stat} properties={properties} />}
          </SingleStatTransform>
        )
      case ViewType.Table:
        return <TimeMachineTables tables={tables} properties={properties} />
      case ViewType.Gauge:
        return <GaugeChart tables={tables} properties={properties} />
      case ViewType.Line:
        return (
          <LineGraph
            tables={tables}
            viewID={viewID}
            onZoom={onZoom}
            loading={loading}
            timeRange={timeRange}
            properties={properties}
          />
        )
      case ViewType.LinePlusSingleStat:
        const lineProperties = {
          ...properties,
          type: ViewType.Line,
        } as LineView

        const singleStatProperties = {
          ...properties,
          type: ViewType.SingleStat,
        } as SingleStatView

        return (
          <LineGraph
            tables={tables}
            viewID={viewID}
            onZoom={onZoom}
            loading={loading}
            timeRange={timeRange}
            properties={lineProperties}
          >
            <SingleStatTransform tables={tables}>
              {stat => (
                <SingleStat stat={stat} properties={singleStatProperties} />
              )}
            </SingleStatTransform>
          </LineGraph>
        )
      case ViewType.StepPlot:
        return (
          <StepPlot
            tables={tables}
            viewID={viewID}
            onZoom={onZoom}
            loading={loading}
            timeRange={timeRange}
            properties={properties}
          />
        )
      case ViewType.Stacked:
        return (
          <Stacked
            tables={tables}
            viewID={viewID}
            onZoom={onZoom}
            loading={loading}
            timeRange={timeRange}
            properties={properties}
          />
        )
      default:
        return <div>YO!</div>
    }
  }
}
