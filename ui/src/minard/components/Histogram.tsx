import React, {SFC} from 'react'

import {PlotEnv} from 'src/minard'
import {bin} from 'src/minard/utils/bin'
import HistogramBars from 'src/minard/components/HistogramBars'
import HistogramTooltip from 'src/minard/components/HistogramTooltip'
import {findHoveredRowIndices} from 'src/minard/utils/findHoveredRowIndices'
import {useLayer} from 'src/minard/utils/useLayer'

export enum Position {
  Stacked = 'stacked',
  Overlaid = 'overlaid',
}

export interface Props {
  env: PlotEnv
  x: string
  fill: string[]
  colors: string[]
  position?: Position
  binCount?: number
  tooltip?: (props: TooltipProps) => JSX.Element
}

export interface TooltipProps {
  xMin: number
  xMax: number
  counts: Array<{
    grouping: {[colName: string]: any}
    count: number
    color: string
  }>
}

export const Histogram: SFC<Props> = ({
  env,
  x,
  fill,
  colors,
  tooltip = null,
  binCount = null,
  position = Position.Stacked,
}: Props) => {
  const baseTable = env.baseLayer.table
  const xDomain = env.xDomain

  const layer = useLayer(
    env,
    () => {
      const [table, aesthetics] = bin(
        baseTable,
        x,
        xDomain,
        fill,
        binCount,
        position
      )

      return {table, aesthetics, colors, scales: {}}
    },
    [baseTable, xDomain, x, fill, position, binCount, colors]
  )

  if (!layer) {
    return null
  }

  const {
    innerWidth,
    innerHeight,
    hoverX,
    hoverY,
    baseLayer: {
      scales: {x: xScale, y: yScale},
    },
  } = env

  const {aesthetics, table} = layer

  const hoveredRowIndices = findHoveredRowIndices(
    table.columns[aesthetics.xMin],
    table.columns[aesthetics.xMax],
    table.columns[aesthetics.yMax],
    hoverX,
    hoverY,
    xScale,
    yScale
  )

  return (
    <>
      <HistogramBars
        width={innerWidth}
        height={innerHeight}
        layer={layer}
        xScale={xScale}
        yScale={yScale}
        position={position}
        hoveredRowIndices={hoveredRowIndices}
      />
      {hoveredRowIndices && (
        <HistogramTooltip
          hoverX={hoverX}
          hoverY={hoverY}
          hoveredRowIndices={hoveredRowIndices}
          layer={layer}
          tooltip={tooltip}
        />
      )}
    </>
  )
}
