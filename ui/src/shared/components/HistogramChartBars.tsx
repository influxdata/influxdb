import React, {PureComponent} from 'react'
import _ from 'lodash'
import {ScaleLinear, ScaleTime} from 'd3-scale'

import {HistogramData, HistogramDatum} from 'src/types/histogram'

const BAR_BORDER_RADIUS = 4
const BAR_PADDING_SIDES = 4

interface Props {
  width: number
  height: number
  data: HistogramData
  xScale: ScaleTime<number, number>
  yScale: ScaleLinear<number, number>
}

class HistogramChartBars extends PureComponent<Props> {
  public render() {
    return this.renderData.map(group => {
      const {key, clip, bars} = group

      return (
        <g key={key} className="histogram-chart-bars--bars">
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
              clipPath={`url(#histogram-chart-bars--clip-${key})`}
              data-group={d.group}
              data-key={d.key}
            />
          ))}
        </g>
      )
    })
  }

  private get renderData() {
    const {data, xScale, yScale} = this.props
    const {barWidth, sortFn} = this

    const visibleData = data.filter(d => d.value !== 0)
    const groups = Object.values(_.groupBy(visibleData, 'time'))

    for (const group of groups) {
      group.sort(sortFn)
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
      }

      let offset = 0

      group.forEach((d: HistogramDatum) => {
        const height = yScale(0) - yScale(d.value)

        renderData.bars.push({
          key: d.key,
          group: d.group,
          x,
          y: yScale(d.value) - offset,
          width: barWidth,
          height,
        })

        offset += height
      })

      return renderData
    })
  }

  private get sortFn() {
    const {data} = this.props

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

  private get barWidth() {
    const {data, xScale, width} = this.props

    const dataInView = data.filter(
      d => xScale(d.time) >= 0 && xScale(d.time) <= width
    )
    const barCount = Object.values(_.groupBy(dataInView, 'time')).length

    return Math.round(width / barCount - BAR_PADDING_SIDES)
  }
}

export default HistogramChartBars
