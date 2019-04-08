// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {AutoSizer} from 'react-virtualized'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import GaugeChart from 'src/shared/components/GaugeChart'
import SingleStat from 'src/shared/components/SingleStat'
import SingleStatTransform from 'src/shared/components/SingleStatTransform'
import TableGraphs from 'src/shared/components/tables/TableGraphs'
import DygraphContainer from 'src/shared/components/DygraphContainer'
import Histogram from 'src/shared/components/Histogram'
import HistogramTransform from 'src/timeMachine/components/HistogramTransform'

// Utils
import {getActiveTimeMachine, getTables} from 'src/timeMachine/selectors'

// Types
import {
  TimeRange,
  ViewType,
  QueryViewProperties,
  FluxTable,
  RemoteDataState,
  XYView,
  XYViewGeom,
  SingleStatView,
  AppState,
} from 'src/types'

interface StateProps {
  files: string[]
  tables: FluxTable[]
  loading: RemoteDataState
  timeRange: TimeRange
  properties: QueryViewProperties
  isViewingRawData: boolean
}

const VisSwitcher: FunctionComponent<StateProps> = ({
  files,
  tables,
  loading,
  timeRange,
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

  switch (properties.type) {
    case ViewType.SingleStat:
      return (
        <SingleStatTransform tables={tables}>
          {stat => <SingleStat stat={stat} properties={properties} />}
        </SingleStatTransform>
      )
    case ViewType.Table:
      return <TableGraphs tables={tables} properties={properties} />
    case ViewType.Gauge:
      return <GaugeChart tables={tables} properties={properties} />
    case ViewType.XY:
      return (
        <DygraphContainer
          viewID="time-machine"
          tables={tables}
          loading={loading}
          timeRange={timeRange}
          properties={properties}
        />
      )
    case ViewType.LinePlusSingleStat:
      const xyProperties: XYView = {
        ...properties,
        colors: properties.colors.filter(c => c.type === 'scale'),
        type: ViewType.XY,
        geom: XYViewGeom.Line,
      }

      const singleStatProperties: SingleStatView = {
        ...properties,
        colors: properties.colors.filter(c => c.type !== 'scale'),
        type: ViewType.SingleStat,
      }

      return (
        <DygraphContainer
          viewID="time-machine"
          tables={tables}
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
    case ViewType.Histogram:
      return (
        <HistogramTransform>
          {({table, xColumn, fillColumns}) => (
            <Histogram
              table={table}
              properties={{...properties, xColumn, fillColumns}}
            />
          )}
        </HistogramTransform>
      )
    default:
      return null
  }
}

const mstp = (state: AppState) => {
  const {
    view: {properties},
    timeRange,
    isViewingRawData,
    queryResults: {status: loading, files},
  } = getActiveTimeMachine(state)

  const tables = getTables(state)

  return {
    files,
    tables,
    loading,
    timeRange,
    properties,
    isViewingRawData,
  }
}

export default connect<StateProps>(mstp)(VisSwitcher)
