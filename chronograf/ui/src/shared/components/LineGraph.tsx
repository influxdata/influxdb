// Libraries
import React, {PureComponent, CSSProperties} from 'react'
import Dygraph from 'src/shared/components/dygraph/Dygraph'

// Components
import {ErrorHandlingWith} from 'src/shared/decorators/errors'
import InvalidData from 'src/shared/components/InvalidData'

// Utils
import {
  fluxTablesToDygraph,
  FluxTablesToDygraphResult,
} from 'src/shared/parsing/flux/dygraph'

// Types
import {ColorString} from 'src/types/colors'
import {DecimalPlaces} from 'src/types/dashboards'
import {Axes, TimeRange, RemoteDataState, FluxTable} from 'src/types'
import {ViewType, CellQuery} from 'src/types/v2'

interface Props {
  axes: Axes
  type: ViewType
  queries: CellQuery[]
  timeRange: TimeRange
  colors: ColorString[]
  loading: RemoteDataState
  decimalPlaces: DecimalPlaces
  data: FluxTable[]
  viewID: string
  cellHeight: number
  staticLegend: boolean
  onZoom: () => void
  handleSetHoverTime: () => void
  activeQueryIndex?: number
}

@ErrorHandlingWith(InvalidData)
class LineGraph extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    staticLegend: false,
  }

  private isValidData: boolean = true
  private timeSeries: FluxTablesToDygraphResult

  public componentWillMount() {
    const {data} = this.props
    this.parseTimeSeries(data)
  }

  public parseTimeSeries(data) {
    this.timeSeries = fluxTablesToDygraph(data)
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
      axes,
      type,
      colors,
      viewID,
      onZoom,
      loading,
      queries,
      timeRange,
      staticLegend,
      handleSetHoverTime,
    } = this.props

    const {labels, dygraphsData} = this.timeSeries

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
          timeSeries={dygraphsData}
          staticLegend={staticLegend}
          dygraphSeries={{}}
          isGraphFilled={this.isGraphFilled}
          containerStyle={this.containerStyle}
          handleSetHoverTime={handleSetHoverTime}
        >
          {type === ViewType.LinePlusSingleStat && (
            <div>Single Stat Goes Here</div>
          )}
        </Dygraph>
      </div>
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

export default LineGraph
