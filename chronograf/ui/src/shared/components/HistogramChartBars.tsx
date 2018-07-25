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
  ColorScale,
  HistogramColor,
  SortFn,
} from 'src/types/histogram'

const BAR_BORDER_RADIUS = 3
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

interface BarGroup {
  key: string
  clip: {
    x: number
    y: number
    width: number
    height: number
  }
  bars: Array<{
    key: string
    group: string
    x: number
    y: number
    width: number
    height: number
    fill: string
  }>
  data: HistogramData
}

const getBarGroups = ({
  data,
  width,
  xScale,
  yScale,
  colorScale,
  hoverData,
  colors,
  sortBarGroups,
}: Partial<Props>): BarGroup[] => {
  const barWidth = getBarWidth({data, xScale, width})
  const visibleData = data.filter(d => d.value !== 0)
  const timeGroups = Object.values(_.groupBy(visibleData, 'time'))

  for (const timeGroup of timeGroups) {
    timeGroup.sort(sortBarGroups)
  }

  let hoverDataKeys = []

  if (!!hoverData) {
    hoverDataKeys = hoverData.data.map(h => h.key)
  }

  return timeGroups.map(timeGroup => {
    const time = timeGroup[0].time
    const x = xScale(time) - barWidth / 2
    const total = _.sumBy(timeGroup, 'value')

    const barGroup = {
      key: `${time}-${total}-${x}`,
      clip: {
        x,
        y: yScale(total),
        width: barWidth,
        height: yScale(0) - yScale(total) + BAR_BORDER_RADIUS,
      },
      bars: [],
      data: timeGroup,
    }

    let offset = 0

    timeGroup.forEach((d: HistogramDatum) => {
      const height = yScale(0) - yScale(d.value)
      const k = hoverDataKeys.includes(d.key) ? HOVER_BRIGTHEN_FACTOR : 0
      const groupColor = colors.find(c => c.group === d.group)
      const fill = color(colorScale(_.get(groupColor, 'color', ''), d.group))
        .brighter(k)
        .hex()

      barGroup.bars.push({
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

    return barGroup
  })
}

interface BarGroup {
  key: string
  clip: {
    x: number
    y: number
    width: number
    height: number
  }
  bars: Array<{
    key: string
    group: string
    x: number
    y: number
    width: number
    height: number
    fill: string
  }>
  data: HistogramData
}
interface Props {
  width: number
  height: number
  data: HistogramData
  xScale: ScaleTime<number, number>
  yScale: ScaleLinear<number, number>
  colorScale: ColorScale
  hoverData?: HoverData
  colors: HistogramColor[]
  onHover: (h: HoverData) => void
  onBarClick?: (time: string) => void
  sortBarGroups: SortFn
}

interface State {
  barGroups: BarGroup[]
}

class HistogramChartBars extends PureComponent<Props, State> {
  public static getDerivedStateFromProps(props: Props) {
    return {barGroups: getBarGroups(props)}
  }

  constructor(props) {
    super(props)

    this.state = {barGroups: []}
  }

  public render() {
    const {barGroups} = this.state

    return barGroups.map(group => {
      const {key, clip, bars} = group

      return (
        <g
          key={key}
          className="histogram-chart-bars--bars"
          data-key={key}
          onMouseOver={this.handleMouseOver}
          onMouseOut={this.handleMouseOut}
          onClick={this.handleBarClick(group.data)}
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

  private handleBarClick = data => (): void => {
    const {onBarClick} = this.props

    if (onBarClick) {
      const time = data[0].time
      onBarClick(time)
    }
  }

  private handleMouseOver = (e: MouseEvent<SVGGElement>): void => {
    const groupKey = getDeep<string>(e, 'currentTarget.dataset.key', '')

    if (!groupKey) {
      return
    }

    const {barGroups} = this.state
    const hoverGroup = barGroups.find(d => d.key === groupKey)

    if (!hoverGroup) {
      return
    }

    const data = _.get(hoverGroup, 'data').reverse()
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
