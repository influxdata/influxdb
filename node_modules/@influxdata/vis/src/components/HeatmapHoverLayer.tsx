import * as React from 'react'
import {useLayoutEffect, useRef, FunctionComponent} from 'react'

import {HeatmapLayerConfig} from '../types'
import {getTooltipData} from '../utils/heatmapTooltip'
import {Tooltip} from './Tooltip'
import {PlotEnv} from '../utils/PlotEnv'
import {drawHeatmap} from '../utils/drawHeatmap'

interface Props {
  layerIndex: number
  env: PlotEnv
  hoveredRowIndices: number[]
}

export const HeatmapHoverLayer: FunctionComponent<Props> = ({
  layerIndex,
  env,
  hoveredRowIndices,
}) => {
  const {
    innerWidth: width,
    innerHeight: height,
    xScale,
    yScale,
    config: {table: configTable, layers},
  } = env

  const {x: xColKey, y: yColKey} = layers[layerIndex] as HeatmapLayerConfig
  const xColName = configTable.getColumnName(xColKey)
  const yColName = configTable.getColumnName(yColKey)
  const table = env.getTable(layerIndex)
  const fillScale = env.getScale(layerIndex, 'fill')

  const tooltipData = getTooltipData(
    hoveredRowIndices,
    xColKey,
    yColKey,
    xColName,
    yColName,
    env.getFormatterForColumn,
    table
  )

  const canvasRef = useRef<HTMLCanvasElement>(null)

  useLayoutEffect(() => {
    return drawHeatmap({
      canvas: canvasRef.current,
      width,
      height,
      table,
      xScale,
      yScale,
      fillScale,
      brighten: 1,
      rows: hoveredRowIndices,
    })
  }, [
    canvasRef.current,
    width,
    height,
    table,
    xScale,
    yScale,
    fillScale,
    hoveredRowIndices,
  ])

  return (
    <>
      <canvas
        className="vis-layer heatmap-interactions"
        ref={canvasRef}
        style={{position: 'absolute', top: 0, left: 0}}
      />
      {tooltipData && <Tooltip data={tooltipData} env={env} />}
    </>
  )
}
