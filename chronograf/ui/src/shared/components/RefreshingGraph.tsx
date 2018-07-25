// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import LineGraph from 'src/shared/components/LineGraph'
import GaugeChart from 'src/shared/components/GaugeChart'
import TableGraph from 'src/shared/components/TableGraph'
import SingleStat from 'src/shared/components/SingleStat'
import TimeSeries from 'src/shared/components/time_series/TimeSeries'

// Constants
import {emptyGraphCopy} from 'src/shared/copy/cell'
import {
  DEFAULT_TIME_FORMAT,
  DEFAULT_DECIMAL_PLACES,
} from 'src/dashboards/constants'

// Actions
import {setHoverTime} from 'src/dashboards/actions'

// Types
import {ColorString} from 'src/types/colors'
import {Source, Axes, TimeRange, Template, Query, CellType} from 'src/types'
import {TableOptions, FieldOption, DecimalPlaces} from 'src/types/dashboards'

interface Props {
  axes: Axes
  source: Source
  queries: Query[]
  timeRange: TimeRange
  colors: ColorString[]
  templates: Template[]
  tableOptions: TableOptions
  fieldOptions: FieldOption[]
  decimalPlaces: DecimalPlaces
  type: CellType
  cellID: string
  inView: boolean
  isInCEO: boolean
  timeFormat: string
  cellHeight: number
  autoRefresh: number
  staticLegend: boolean
  manualRefresh: number
  resizerTopHeight: number
  onZoom: () => void
  editQueryStatus: () => void
  onSetResolution: () => void
  grabDataForDownload: () => void
  handleSetHoverTime: () => void
}

class RefreshingGraph extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    inView: true,
    manualRefresh: 0,
    staticLegend: false,
    timeFormat: DEFAULT_TIME_FORMAT,
    decimalPlaces: DEFAULT_DECIMAL_PLACES,
  }

  public render() {
    const {
      inView,
      type,
      queries,
      source,
      templates,
      editQueryStatus,
    } = this.props

    if (!queries.length) {
      return (
        <div className="graph-empty">
          <p data-test="data-explorer-no-results">{emptyGraphCopy}</p>
        </div>
      )
    }

    return (
      <TimeSeries
        source={source}
        inView={inView}
        queries={this.queries}
        templates={templates}
        editQueryStatus={editQueryStatus}
      >
        {({timeSeries, loading}) => {
          switch (type) {
            case CellType.SingleStat:
              return this.singleStat(timeSeries)
            case CellType.Table:
              return this.table(timeSeries)
            case CellType.Gauge:
              return this.gauge(timeSeries)
            default:
              return this.lineGraph(timeSeries, loading)
          }
        }}
      </TimeSeries>
    )
  }

  private singleStat = (data): JSX.Element => {
    const {colors, cellHeight, decimalPlaces, manualRefresh} = this.props

    return (
      <SingleStat
        data={data}
        colors={colors}
        prefix={this.prefix}
        suffix={this.suffix}
        lineGraph={false}
        key={manualRefresh}
        cellHeight={cellHeight}
        decimalPlaces={decimalPlaces}
      />
    )
  }

  private table = (data): JSX.Element => {
    const {
      colors,
      fieldOptions,
      timeFormat,
      tableOptions,
      decimalPlaces,
      manualRefresh,
      handleSetHoverTime,
      grabDataForDownload,
      isInCEO,
    } = this.props

    return (
      <TableGraph
        data={data}
        colors={colors}
        isInCEO={isInCEO}
        key={manualRefresh}
        tableOptions={tableOptions}
        fieldOptions={fieldOptions}
        timeFormat={timeFormat}
        decimalPlaces={decimalPlaces}
        grabDataForDownload={grabDataForDownload}
        handleSetHoverTime={handleSetHoverTime}
      />
    )
  }

  private gauge = (data): JSX.Element => {
    const {
      colors,
      cellID,
      cellHeight,
      decimalPlaces,
      manualRefresh,
      resizerTopHeight,
    } = this.props

    return (
      <GaugeChart
        data={data}
        cellID={cellID}
        colors={colors}
        prefix={this.prefix}
        suffix={this.suffix}
        key={manualRefresh}
        cellHeight={cellHeight}
        decimalPlaces={decimalPlaces}
        resizerTopHeight={resizerTopHeight}
      />
    )
  }

  private lineGraph = (data, loading): JSX.Element => {
    const {
      axes,
      type,
      colors,
      onZoom,
      cellID,
      queries,
      timeRange,
      cellHeight,
      decimalPlaces,
      staticLegend,
      manualRefresh,
      handleSetHoverTime,
    } = this.props

    return (
      <LineGraph
        data={data}
        type={type}
        axes={axes}
        cellID={cellID}
        colors={colors}
        onZoom={onZoom}
        queries={queries}
        loading={loading}
        key={manualRefresh}
        timeRange={timeRange}
        cellHeight={cellHeight}
        staticLegend={staticLegend}
        decimalPlaces={decimalPlaces}
        handleSetHoverTime={handleSetHoverTime}
      />
    )
  }

  private get queries(): Query[] {
    const {queries, type} = this.props
    if (type === CellType.SingleStat) {
      return [queries[0]]
    }

    if (type === CellType.Gauge) {
      return [queries[0]]
    }

    return queries
  }

  private get prefix(): string {
    const {axes} = this.props

    return _.get(axes, 'y.prefix', '')
  }

  private get suffix(): string {
    const {axes} = this.props
    return _.get(axes, 'y.suffix', '')
  }
}

const mapStateToProps = ({annotations: {mode}}) => ({
  mode,
})

const mdtp = {
  handleSetHoverTime: setHoverTime,
}

export default connect(mapStateToProps, mdtp)(RefreshingGraph)
