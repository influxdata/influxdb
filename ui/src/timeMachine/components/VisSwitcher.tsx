// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {AutoSizer} from 'react-virtualized'
import {Plot} from '@influxdata/vis'

// Components
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import GaugeChart from 'src/shared/components/GaugeChart'
import SingleStat from 'src/shared/components/SingleStat'
import SingleStatTransform from 'src/shared/components/SingleStatTransform'
import TableGraphs from 'src/shared/components/tables/TableGraphs'
import HistogramContainer from 'src/shared/components/HistogramContainer'
import HistogramTransform from 'src/timeMachine/components/HistogramTransform'
import XYContainer from 'src/shared/components/XYContainer'

// Utils
import {getActiveTimeMachine, getTables} from 'src/timeMachine/selectors'

// Types
import {
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
        <XYContainer
          files={files}
          viewProperties={properties}
          loading={loading}
        >
          {config => <Plot config={config} />}
        </XYContainer>
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
        <XYContainer
          files={files}
          viewProperties={xyProperties}
          loading={loading}
        >
          {config => (
            <Plot config={config}>
              <SingleStatTransform tables={tables}>
                {stat => (
                  <SingleStat stat={stat} properties={singleStatProperties} />
                )}
              </SingleStatTransform>
            </Plot>
          )}
        </XYContainer>
      )

    case ViewType.Histogram:
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
    default:
      return null
  }
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
