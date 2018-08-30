// Libraries
import React, {PureComponent, CSSProperties} from 'react'
import Dygraph from 'src/shared/components/Dygraph'
import {withRouter, RouteComponentProps} from 'react-router'
import _ from 'lodash'

// Components
import SingleStat from 'src/shared/components/SingleStat'
import {ErrorHandlingWith} from 'src/shared/decorators/errors'
import InvalidData from 'src/shared/components/InvalidData'

// Utils
import {
  timeSeriesToDygraph,
  TimeSeriesToDyGraphReturnType,
} from 'src/utils/timeSeriesTransformers'

// Types
import {ColorString} from 'src/types/colors'
import {DecimalPlaces} from 'src/types/dashboards'
import {TimeSeriesServerResponse} from 'src/types/series'
import {Axes, TimeRange, RemoteDataState} from 'src/types'
import {ViewType, CellQuery} from 'src/types/v2'

interface Props {
  axes: Axes
  type: ViewType
  queries: CellQuery[]
  timeRange: TimeRange
  colors: ColorString[]
  loading: RemoteDataState
  decimalPlaces: DecimalPlaces
  data: TimeSeriesServerResponse[]
  viewID: string
  cellHeight: number
  staticLegend: boolean
  onZoom: () => void
  handleSetHoverTime: () => void
  activeQueryIndex?: number
}

type LineGraphProps = Props & RouteComponentProps<any, any>

@ErrorHandlingWith(InvalidData)
class LineGraph extends PureComponent<LineGraphProps> {
  public static defaultProps: Partial<LineGraphProps> = {
    staticLegend: false,
  }

  private isValidData: boolean = true
  private timeSeries: TimeSeriesToDyGraphReturnType

  public componentWillMount() {
    const {data} = this.props
    this.parseTimeSeries(data)
  }

  public parseTimeSeries(data) {
    const {location} = this.props

    this.timeSeries = timeSeriesToDygraph(data, location.pathname)
    const timeSeries = _.get(this.timeSeries, 'timeSeries', [])
    this.isValidData = this.validateTimeSeries(timeSeries)
  }

  public componentWillUpdate(nextProps) {
    const {data, activeQueryIndex} = this.props
    if (
      data !== nextProps.data ||
      activeQueryIndex !== nextProps.activeQueryIndex
    ) {
      this.parseTimeSeries(nextProps.data)
    }
  }

  public render() {
    if (!this.isValidData) {
      return <InvalidData />
    }

    const {
      data,
      axes,
      type,
      colors,
      viewID,
      onZoom,
      loading,
      queries,
      timeRange,
      cellHeight,
      staticLegend,
      decimalPlaces,
      handleSetHoverTime,
    } = this.props

    const {labels, timeSeries, dygraphSeries} = this.timeSeries

    const options = {
      rightGap: 0,
      yRangePad: 10,
      labelsKMB: true,
      fillGraph: true,
      axisLabelWidth: 60,
      animatedZooms: true,
      drawAxesAtZero: true,
      axisLineColor: '#383846',
      gridLineColor: '#383846',
      connectSeparatedPoints: true,
      stepPlot: type === 'line-stepplot',
      stackedGraph: type === 'line-stacked',
    }

    return (
      <div className="dygraph graph--hasYLabel" style={this.style}>
        {loading === RemoteDataState.Loading && <GraphLoadingDots />}
        <Dygraph
          type={type}
          axes={axes}
          viewID={viewID}
          colors={colors}
          onZoom={onZoom}
          labels={labels}
          queries={queries}
          options={options}
          timeRange={timeRange}
          timeSeries={timeSeries}
          staticLegend={staticLegend}
          dygraphSeries={dygraphSeries}
          isGraphFilled={this.isGraphFilled}
          containerStyle={this.containerStyle}
          handleSetHoverTime={handleSetHoverTime}
        >
          {type === ViewType.LinePlusSingleStat && (
            <SingleStat
              data={data}
              lineGraph={true}
              colors={colors}
              prefix={this.prefix}
              suffix={this.suffix}
              cellHeight={cellHeight}
              decimalPlaces={decimalPlaces}
            />
          )}
        </Dygraph>
      </div>
    )
  }

  private validateTimeSeries = ts => {
    return _.every(ts, r =>
      _.every(
        r,
        (v, i: number) =>
          (i === 0 && Date.parse(v)) || _.isNumber(v) || _.isNull(v)
      )
    )
  }

  private get isGraphFilled(): boolean {
    const {type} = this.props

    if (type === ViewType.LinePlusSingleStat) {
      return false
    }

    return true
  }

  private get style(): CSSProperties {
    return {height: '100%'}
  }

  private get prefix(): string {
    const {axes} = this.props

    if (!axes) {
      return ''
    }

    return axes.y.prefix
  }

  private get suffix(): string {
    const {axes} = this.props

    if (!axes) {
      return ''
    }

    return axes.y.suffix
  }

  private get containerStyle(): CSSProperties {
    return {
      width: 'calc(100% - 32px)',
      height: 'calc(100% - 16px)',
      position: 'absolute',
      top: '8px',
    }
  }
}

const GraphLoadingDots = () => (
  <div className="graph-panel__refreshing">
    <div />
    <div />
    <div />
  </div>
)

export default withRouter<Props>(LineGraph)
