import React, {PureComponent, MouseEvent} from 'react'
import _ from 'lodash'
import {scaleLinear, scaleTime, ScaleLinear, ScaleTime} from 'd3-scale'

import HistogramChartAxes from 'src/shared/components/HistogramChartAxes'
import HistogramChartBars from 'src/shared/components/HistogramChartBars'
import HistogramChartTooltip from 'src/shared/components/HistogramChartTooltip'
import HistogramChartSkeleton from 'src/shared/components/HistogramChartSkeleton'
import XBrush from 'src/shared/components/XBrush'

import extentBy from 'src/utils/extentBy'
import {getDeep} from 'src/utils/wrappers'

import {RemoteDataState} from 'src/types'
import {
  TimePeriod,
  HistogramData,
  HistogramDatum,
  Margins,
  TooltipAnchor,
} from 'src/types/histogram'

const PADDING_TOP = 0.2
const TOOLTIP_HORIZONTAL_MARGIN = 5
const TOOLTIP_REFLECT_DIST = 100

// Rather than use these magical constants, we could also render a digit and
// capture its measured width with as state before rendering anything else.
// Doing so would be robust but overkill.
const DIGIT_WIDTH = 7
const PERIOD_DIGIT_WIDTH = 4

interface Props {
  data: HistogramData
  dataStatus: RemoteDataState
  width: number
  height: number
  onZoom: (TimePeriod) => void
}

interface State {
  hoverX: number
  hoverY: number
  hoverDatum?: HistogramDatum
  hoverAnchor: TooltipAnchor
}

class HistogramChart extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {hoverX: -1, hoverY: -1, hoverAnchor: 'left'}
  }

  public render() {
    const {width, height, data} = this.props
    const {margins} = this

    if (width === 0 || height === 0) {
      return null
    }

    if (!data.length) {
      return (
        <HistogramChartSkeleton
          width={width}
          height={height}
          margins={margins}
        />
      )
    }

    const {hoverDatum, hoverX, hoverY, hoverAnchor} = this.state
    const {
      xScale,
      yScale,
      adjustedWidth,
      adjustedHeight,
      bodyTransform,
      loadingClass,
    } = this

    return (
      <>
        <svg
          width={width}
          height={height}
          className={`histogram-chart ${loadingClass}`}
          onMouseOver={this.handleMouseMove}
          onMouseOut={this.handleMouseOut}
        >
          <defs>
            <clipPath id="histogram-chart--bars-clip">
              <rect x="0" y="0" width={adjustedWidth} height={adjustedHeight} />
            </clipPath>
          </defs>
          <g className="histogram-chart--axes">
            <HistogramChartAxes
              width={width}
              height={height}
              margins={margins}
              xScale={xScale}
              yScale={yScale}
            />
          </g>
          <g className="histogram-chart--brush" transform={bodyTransform}>
            <XBrush
              xScale={xScale}
              width={adjustedWidth}
              height={adjustedHeight}
              onBrush={this.handleBrush}
            />
          </g>
          <g
            transform={bodyTransform}
            className="histogram-chart--bars"
            clipPath="url(#histogram-chart--bars-clip)"
          >
            <HistogramChartBars
              width={adjustedWidth}
              height={adjustedHeight}
              data={data}
              xScale={xScale}
              yScale={yScale}
            />
          </g>
        </svg>
        <HistogramChartTooltip
          datum={hoverDatum}
          x={hoverX}
          y={hoverY}
          anchor={hoverAnchor}
        />
      </>
    )
  }

  private get xScale(): ScaleTime<number, number> {
    const {adjustedWidth} = this
    const {data} = this.props

    const [t0, t1] = extentBy(data, d => d.time)

    return scaleTime()
      .domain([new Date(t0.time), new Date(t1.time)])
      .range([0, adjustedWidth])
  }

  private get yScale(): ScaleLinear<number, number> {
    const {adjustedHeight, maxAggregateCount} = this

    return scaleLinear()
      .domain([0, maxAggregateCount + PADDING_TOP * maxAggregateCount])
      .range([adjustedHeight, 0])
  }

  private get adjustedWidth(): number {
    const {margins} = this

    return this.props.width - margins.left - margins.right
  }

  private get adjustedHeight(): number {
    const {margins} = this

    return this.props.height - margins.top - margins.bottom
  }

  private get bodyTransform(): string {
    const {margins} = this

    return `translate(${margins.left}, ${margins.top})`
  }

  private get margins(): Margins {
    const {maxAggregateCount} = this

    const domainTop = maxAggregateCount + PADDING_TOP * maxAggregateCount
    const left = domainTop.toString().length * DIGIT_WIDTH + PERIOD_DIGIT_WIDTH

    return {top: 5, right: 0, bottom: 20, left}
  }

  private get maxAggregateCount(): number {
    const {data} = this.props

    if (!data.length) {
      return 0
    }

    const groups = _.groupBy(data, 'time')
    const counts = Object.values(groups).map(group =>
      group.reduce((sum, current) => sum + current.value, 0)
    )

    return Math.max(...counts)
  }

  private get loadingClass(): string {
    const {dataStatus} = this.props

    return dataStatus === RemoteDataState.Loading ? 'loading' : ''
  }

  private handleBrush = (t: TimePeriod): void => {
    this.props.onZoom(t)
    this.setState({hoverDatum: null})
  }

  private handleMouseMove = (e: MouseEvent<SVGElement>): void => {
    const key = getDeep<string>(e, 'target.dataset.key', '')

    if (!key) {
      return
    }

    const {data} = this.props
    const hoverDatum = data.find(d => d.key === key)

    if (!hoverDatum) {
      return
    }

    const bar = e.target as SVGRectElement
    const barRect = bar.getBoundingClientRect()
    const barRectHeight = barRect.bottom - barRect.top
    const hoverY = barRect.top + barRectHeight / 2

    let hoverX = barRect.right + TOOLTIP_HORIZONTAL_MARGIN
    let hoverAnchor: TooltipAnchor = 'left'

    if (hoverX >= window.innerWidth - TOOLTIP_REFLECT_DIST) {
      hoverX = window.innerWidth - barRect.left + TOOLTIP_HORIZONTAL_MARGIN
      hoverAnchor = 'right'
    }

    this.setState({hoverDatum, hoverX, hoverY, hoverAnchor})
  }

  private handleMouseOut = (): void => {
    this.setState({hoverDatum: null})
  }
}

export default HistogramChart
