import React, {useState, useEffect, SFC} from 'react'
import uuid from 'uuid'

import {PlotEnv} from 'src/minard'
import {bin} from 'src/minard/utils/bin'
import {registerLayer, unregisterLayer} from 'src/minard/utils/plotEnvActions'
import HistogramBars from 'src/minard/components/HistogramBars'
import HistogramTooltip from 'src/minard/components/HistogramTooltip'
import {findHoveredRowIndices} from 'src/minard/utils/findHoveredRowIndices'

export enum Position {
  Stacked = 'stacked',
  Overlaid = 'overlaid',
}

export interface Props {
  env: PlotEnv
  x?: string
  fill?: string[]
  position?: Position
  binCount?: number
  colors?: string[]
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

export const Histogram: SFC<Props> = props => {
  const [layerKey] = useState(() => uuid.v4())

  const {binCount, position} = props
  const {layers, defaults, dispatch} = props.env
  const layer = layers[layerKey]
  const table = defaults.table
  const x = props.x || defaults.aesthetics.x
  const fill = props.fill || defaults.aesthetics.fill
  const colors = props.colors

  useEffect(
    () => {
      const [statTable, mappings] = bin(table, x, fill, binCount, position)

      dispatch(registerLayer(layerKey, statTable, mappings, colors))

      return () => dispatch(unregisterLayer(layerKey))
    },
    [table, x, fill, position, binCount, colors]
  )

  if (!layer) {
    return null
  }

  const {
    innerWidth,
    innerHeight,
    hoverX,
    hoverY,
    defaults: {
      scales: {x: xScale, y: yScale},
    },
  } = props.env

  const {aesthetics, table: statTable} = layer

  let hoveredRowIndices = null

  if (hoverX && hoverY) {
    hoveredRowIndices = findHoveredRowIndices(
      statTable.columns[aesthetics.xMin],
      statTable.columns[aesthetics.xMax],
      statTable.columns[aesthetics.yMax],
      xScale.invert(hoverX),
      yScale.invert(hoverY)
    )
  }

  return (
    <>
      <HistogramBars
        width={innerWidth}
        height={innerHeight}
        layer={layer}
        xScale={xScale}
        yScale={yScale}
        position={props.position || Position.Stacked}
        hoveredRowIndices={hoveredRowIndices}
      />
      {hoveredRowIndices && (
        <HistogramTooltip
          hoverX={hoverX}
          hoverY={hoverY}
          hoveredRowIndices={hoveredRowIndices}
          width={innerWidth}
          height={innerHeight}
          layer={layer}
          tooltip={props.tooltip}
        />
      )}
    </>
  )
}
