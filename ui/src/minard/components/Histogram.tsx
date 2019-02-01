import React, {useState, useEffect, SFC} from 'react'
import uuid from 'uuid'

import {PlotEnv} from 'src/minard'
import * as stats from 'src/minard/utils/stats'
import {assert} from 'src/minard/utils/assert'
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
  fill?: string
  position?: Position
  bins?: number
  colors?: string[]
  tooltip?: (props: TooltipProps) => JSX.Element
}

export interface TooltipProps {
  x: string
  fill: string
  xMin: number
  xMax: number
  counts: Array<{fill: string | number | boolean; count: number; color: string}>
}

export const Histogram: SFC<Props> = props => {
  const [layerKey] = useState(() => uuid.v4())

  const {bins, position} = props
  const {layers, defaults, dispatch} = props.env
  const layer = layers[layerKey]
  const table = defaults.table
  const x = props.x || defaults.aesthetics.x
  const fill = props.fill || defaults.aesthetics.fill
  const colors = props.colors

  useEffect(
    () => {
      const xCol = table.columns[x]
      const xColType = table.columnTypes[x]
      const fillCol = table.columns[fill]
      const fillColType = table.columnTypes[fill]

      assert('expected an `x` aesthetic', !!x)
      assert(`table does not contain column "${x}"`, !!xCol)

      const [statTable, mappings] = stats.bin(
        xCol,
        xColType,
        fillCol,
        fillColType,
        bins,
        position
      )

      dispatch(registerLayer(layerKey, statTable, mappings, colors))

      return () => dispatch(unregisterLayer(layerKey))
    },
    [table, x, fill, position, bins, colors]
  )

  if (!layer) {
    return null
  }

  const {
    innerWidth,
    innerHeight,
    defaults: {
      scales: {x: xScale, y: yScale, fill: layerFillScale},
    },
  } = props.env

  const {
    aesthetics,
    table: {columns},
    scales: {fill: defaultFillScale},
  } = layer

  const fillScale = layerFillScale || defaultFillScale
  const xMinCol = columns[aesthetics.xMin]
  const xMaxCol = columns[aesthetics.xMax]
  const yMinCol = columns[aesthetics.yMin]
  const yMaxCol = columns[aesthetics.yMax]
  const fillCol = columns[aesthetics.fill]

  const {hoverX, hoverY} = props.env

  let hoveredRowIndices = null

  if (hoverX && hoverY) {
    hoveredRowIndices = findHoveredRowIndices(
      xMinCol,
      xMaxCol,
      yMaxCol,
      xScale.invert(hoverX),
      yScale.invert(hoverY)
    )
  }

  return (
    <>
      <HistogramBars
        width={innerWidth}
        height={innerHeight}
        xMinCol={xMinCol}
        xMaxCol={xMaxCol}
        yMinCol={yMinCol}
        yMaxCol={yMaxCol}
        fillCol={fillCol}
        fillScale={fillScale}
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
          xMinCol={xMinCol}
          xMaxCol={xMaxCol}
          yMinCol={yMinCol}
          yMaxCol={yMaxCol}
          fillCol={fillCol}
          fillScale={fillScale}
          x={x}
          fill={fill}
          tooltip={props.tooltip}
        />
      )}
    </>
  )
}
