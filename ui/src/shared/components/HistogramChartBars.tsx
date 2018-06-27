import React, {PureComponent, MouseEvent} from 'react'
import _ from 'lodash'
import {ScaleLinear, ScaleTime} from 'd3-scale'
import {color} from 'd3-color'

import {getDeep} from 'src/utils/wrappers'

import {
  HistogramData,
  HistogramDatum,
  HoverData,
  TooltipAnchor,
} from 'src/types/histogram'

const BAR_BORDER_RADIUS = 4
const BAR_PADDING_SIDES = 4
const HOVER_BRIGTHEN_FACTOR = 0.4
const TOOLTIP_HORIZONTAL_MARGIN = 5
const TOOLTIP_REFLECT_DIST = 100

const getBarWidth = ({data, xScale, width}): number => {
  const dataInView = data.filter(
    d => xScale(d.time) >= 0 && xScale(d.time) <= width
  )
  const barCount = Object.values(_.groupBy(dataInView, 'time')).length

  return Math.round(width / barCount - BAR_PADDING_SIDES)
}

type SortFn = (a: HistogramDatum, b: HistogramDatum) => number

const getSortFn = (data: HistogramData): SortFn => {
  const counts = {}

  for (const d of data) {
    if (counts[d.group]) {
      counts[d.group] += d.value
    } else {
      counts[d.group] = d.value
    }
  }

  return (a, b) => counts[b.group] - counts[a.group]
}

interface RenderDataBar {
  key: string
  group: string
  x: number
  y: number
  width: number
  height: number
  fill: string
}

interface RenderDataGroup {
  key: string
  clip: {
    x: number
    y: number
    width: number
    height: number
  }
  bars: RenderDataBar[]
  data: HistogramData
}

type RenderData = RenderDataGroup[]

const getRenderData = ({
  data,
  width,
  xScale,
  yScale,
  colorScale,
  hoverData,
}): RenderData => {
  const barWidth = getBarWidth({data, xScale, width})
  const sortFn = getSortFn(data)
  const visibleData = data.filter(d => d.value !== 0)
  const groups = Object.values(_.groupBy(visibleData, 'time'))

  for (const group of groups) {
    group.sort(sortFn)
  }

  let hoverDataKeys = []

  if (!!hoverData) {
    hoverDataKeys = hoverData.data.map(h => h.key)
  }

  return groups.map(group => {
    const time = group[0].time
    const x = xScale(time) - barWidth / 2
    const groupTotal = _.sumBy(group, 'value')

    const renderData = {
      key: `${time}-${groupTotal}-${x}`,
      clip: {
        x,
        y: yScale(groupTotal),
        width: barWidth,
        height: yScale(0) - yScale(groupTotal) + BAR_BORDER_RADIUS,
      },
      bars: [],
      data: group,
    }

    let offset = 0

    group.forEach((d: HistogramDatum) => {
      const height = yScale(0) - yScale(d.value)
      const k = hoverDataKeys.includes(d.key) ? HOVER_BRIGTHEN_FACTOR : 0
      const fill = color(colorScale(d.group))
        .brighter(k)
        .hex()

      renderData.bars.push({
        key: d.key,
        group: d.group,
        x,
        y: yScale(d.value) - offset,
        width: barWidth,
        height,
        fill,
      })

      offset += height
    })

    return renderData
  })
}

interface Props {
  width: number
  height: number
  data: HistogramData
  xScale: ScaleTime<number, number>
  yScale: ScaleLinear<number, number>
  colorScale: (group: string) => string
  hoverData?: HoverData
  onHover: (h: HoverData) => void
}

interface State {
  renderData: RenderData
}

class HistogramChartBars extends PureComponent<Props, State> {
  public static getDerivedStateFromProps(props) {
    return {renderData: getRenderData(props)}
  }

  constructor(props) {
    super(props)

    this.state = {renderData: []}
  }

  public render() {
    const {renderData} = this.state

    return renderData.map(group => {
      const {key, clip, bars} = group

      return (
        <g
          key={key}
          className="histogram-chart-bars--bars"
          data-key={key}
          onMouseOver={this.handleMouseOver}
          onMouseOut={this.handleMouseOut}
        >
          <defs>
            <clipPath id={`histogram-chart-bars--clip-${key}`}>
              <rect
                x={clip.x}
                y={clip.y}
                width={clip.width}
                height={clip.height}
                rx={BAR_BORDER_RADIUS}
                ry={BAR_BORDER_RADIUS}
              />
            </clipPath>
          </defs>
          {bars.map(d => (
            <rect
              key={d.key}
              className="histogram-chart-bars--bar"
              x={d.x}
              y={d.y}
              width={d.width}
              height={d.height}
              fill={d.fill}
              clipPath={`url(#histogram-chart-bars--clip-${key})`}
              data-group={d.group}
              data-key={d.key}
            />
          ))}
        </g>
      )
    })
  }

  private handleMouseOver = (e: MouseEvent<SVGGElement>): void => {
    const groupKey = getDeep<string>(e, 'currentTarget.dataset.key', '')

    if (!groupKey) {
      return
    }

    const {renderData} = this.state
    const hoverGroup = renderData.find(d => d.key === groupKey)

    if (!hoverGroup) {
      return
    }

    const {data} = hoverGroup

    const barGroup = e.currentTarget as SVGGElement
    const boundingRect = barGroup.getBoundingClientRect()
    const boundingRectHeight = boundingRect.bottom - boundingRect.top
    const y = boundingRect.top + boundingRectHeight / 2

    let x = boundingRect.right + TOOLTIP_HORIZONTAL_MARGIN
    let anchor: TooltipAnchor = 'left'

    // This makes an assumption that the component is within the viewport
    if (x >= window.innerWidth - TOOLTIP_REFLECT_DIST) {
      x = window.innerWidth - boundingRect.left + TOOLTIP_HORIZONTAL_MARGIN
      anchor = 'right'
    }

    this.props.onHover({data, x, y, anchor})
  }

  private handleMouseOut = (): void => {
    this.props.onHover(null)
  }
}

export default HistogramChartBars
