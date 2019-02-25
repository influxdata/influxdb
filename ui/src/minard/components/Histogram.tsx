import React, {SFC} from 'react'

import {PlotEnv, HistogramLayer} from 'src/minard'
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
      const [table, mappings] = bin(
        baseTable,
        x,
        xDomain,
        fill,
        binCount,
        position
      )

      return {type: 'histogram', table, mappings, colors}
    },
    [baseTable, xDomain, x, fill, position, binCount, colors]
  ) as HistogramLayer

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

  const {table} = layer

  const hoveredRowIndices = findHoveredRowIndices(
    table,
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
