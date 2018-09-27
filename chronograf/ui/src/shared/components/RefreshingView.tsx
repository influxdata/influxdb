// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import LineGraph from 'src/shared/components/LineGraph'
import StepPlot from 'src/shared/components/StepPlot'
import Stacked from 'src/shared/components/Stacked'
import GaugeChart from 'src/shared/components/GaugeChart'
// import TableGraph from 'src/shared/components/TableGraph'
import SingleStat from 'src/shared/components/SingleStat'
import TimeSeries from 'src/shared/components/time_series/TimeSeries'
import SingleStatTransform from 'src/shared/components/SingleStatTransform'

// Constants
import {emptyGraphCopy} from 'src/shared/copy/cell'
// import {DEFAULT_TIME_FORMAT} from 'src/dashboards/constants'

// Utils
import {buildQueries} from 'src/utils/buildQueriesForLayouts'

// Actions
import {setHoverTime} from 'src/dashboards/actions/v2/hoverTime'

// Types
import {TimeRange, Template, CellQuery} from 'src/types'
import {RefreshingViewProperties, ViewType} from 'src/types/v2/dashboards'

interface Props {
  link: string
  timeRange: TimeRange
  templates: Template[]
  viewID: string
  inView: boolean
  timeFormat: string
  autoRefresh: number
  manualRefresh: number
  staticLegend: boolean
  onZoom: () => void
  editQueryStatus: () => void
  onSetResolution: () => void
  grabDataForDownload: () => void
  handleSetHoverTime: () => void
  properties: RefreshingViewProperties
}

class RefreshingView extends PureComponent<Props & WithRouterProps> {
  public static defaultProps: Partial<Props> = {
    inView: true,
    manualRefresh: 0,
    staticLegend: false,
  }

  public render() {
    const {
      link,
      inView,
      onZoom,
      viewID,
      timeRange,
      templates,
      properties,
      staticLegend,
      manualRefresh,
      handleSetHoverTime,
    } = this.props

    if (!properties.queries.length) {
      return (
        <div className="graph-empty">
          <p data-test="data-explorer-no-results">{emptyGraphCopy}</p>
        </div>
      )
    }

    return (
      <TimeSeries
        link={link}
        inView={inView}
        queries={this.queries}
        templates={templates}
      >
        {({tables, loading}) => {
          switch (properties.type) {
            case ViewType.SingleStat:
              return (
                <SingleStatTransform tables={tables}>
                  {stat => <SingleStat stat={stat} properties={properties} />}
                </SingleStatTransform>
              )
            case ViewType.Table:
              return <div>YO! Imma table</div>
            case ViewType.Gauge:
              return (
                <GaugeChart
                  tables={tables}
                  key={manualRefresh}
                  properties={properties}
                />
              )
            case ViewType.Line:
              return (
                <LineGraph
                  tables={tables}
                  viewID={viewID}
                  onZoom={onZoom}
                  loading={loading}
                  key={manualRefresh}
                  timeRange={timeRange}
                  properties={properties}
                  staticLegend={staticLegend}
                  handleSetHoverTime={handleSetHoverTime}
                />
              )
            case ViewType.StepPlot:
              return (
                <StepPlot
                  tables={tables}
                  viewID={viewID}
                  onZoom={onZoom}
                  loading={loading}
                  key={manualRefresh}
                  timeRange={timeRange}
                  properties={properties}
                  staticLegend={staticLegend}
                  handleSetHoverTime={handleSetHoverTime}
                />
              )
            case ViewType.Stacked:
              return (
                <Stacked
                  tables={tables}
                  viewID={viewID}
                  onZoom={onZoom}
                  loading={loading}
                  key={manualRefresh}
                  timeRange={timeRange}
                  properties={properties}
                  staticLegend={staticLegend}
                  handleSetHoverTime={handleSetHoverTime}
                />
              )
            default:
              return <div>YO!</div>
          }
        }}
      </TimeSeries>
    )
  }

  private get queries(): CellQuery[] {
    const {timeRange, properties} = this.props
    const {type} = properties
    const queries = buildQueries(properties.queries, timeRange)

    if (type === ViewType.SingleStat) {
      return [queries[0]]
    }

    if (type === ViewType.Gauge) {
      return [queries[0]]
    }

    return queries
  }
}

const mstp = ({sources, routing}): Partial<Props> => {
  const sourceID = routing.locationBeforeTransitions.query.sourceID
  const source = sources.find(s => s.id === sourceID)
  const link = source.links.query

  return {
    link,
  }
}

const mdtp = {
  handleSetHoverTime: setHoverTime,
}

export default connect(mstp, mdtp)(withRouter(RefreshingView))
