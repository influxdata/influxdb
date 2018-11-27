// Libraries
import React, {PureComponent} from 'react'

// Components
import GaugeChart from 'src/shared/components/GaugeChart'
import SingleStat from 'src/shared/components/SingleStat'
import SingleStatTransform from 'src/shared/components/SingleStatTransform'
import TimeMachineTables from 'src/shared/components/tables/TimeMachineTables'
import DygraphContainer from 'src/shared/components/DygraphContainer'

// Types
import {
  QueryViewProperties,
  ViewType,
  SingleStatView,
  XYView,
  XYViewGeom,
} from 'src/types/v2/dashboards'
import {FluxTable, RemoteDataState, TimeRange} from 'src/types'

interface Props {
  viewID: string
  tables: FluxTable[]
  loading: RemoteDataState
  properties: QueryViewProperties
  timeRange?: TimeRange
  onZoom?: (range: TimeRange) => void
}

export default class QueryViewSwitcher extends PureComponent<Props> {
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
      case ViewType.XY:
        return (
          <DygraphContainer
            tables={tables}
            viewID={viewID}
            onZoom={onZoom}
            loading={loading}
            timeRange={timeRange}
            properties={properties}
          />
        )
      case ViewType.LinePlusSingleStat:
        const xyProperties = {
          ...properties,
          type: ViewType.XY,
          geom: XYViewGeom.Line,
        } as XYView

        const singleStatProperties = {
          ...properties,
          type: ViewType.SingleStat,
        } as SingleStatView

        return (
          <DygraphContainer
            tables={tables}
            viewID={viewID}
            onZoom={onZoom}
            loading={loading}
            timeRange={timeRange}
            properties={xyProperties}
          >
            <SingleStatTransform tables={tables}>
              {stat => (
                <SingleStat stat={stat} properties={singleStatProperties} />
              )}
            </SingleStatTransform>
          </DygraphContainer>
        )
      default:
        return <div>YO!</div>
    }
  }
}
