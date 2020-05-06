import * as React from 'react'
import {useRef, useLayoutEffect, FunctionComponent} from 'react'

import {HeatmapHoverLayer} from './HeatmapHoverLayer'
import {PlotEnv} from '../utils/PlotEnv'
import {findHoveredRowIndices} from '../utils/heatmapTooltip'
import {drawHeatmap} from '../utils/drawHeatmap'

interface Props {
  layerIndex: number
  env: PlotEnv
  hoverX: number
  hoverY: number
}

export const HeatmapLayer: FunctionComponent<Props> = ({
  layerIndex,
  env,
  hoverX,
  hoverY,
}) => {
  const {innerWidth: width, innerHeight: height, xScale, yScale} = env
  const fillScale = env.getScale(layerIndex, 'fill')
  const table = env.getTable(layerIndex)

  let hoveredRowIndices = null

  if (hoverX && hoverY) {
    hoveredRowIndices = findHoveredRowIndices(
      table,
      xScale.invert(hoverX),
      yScale.invert(hoverY)
    )
  }

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
    })
  }, [canvasRef.current, width, height, table, xScale, yScale, fillScale])

  return (
    <>
      <canvas className="vis-layer heatmap" ref={canvasRef} />
      {hoveredRowIndices && hoveredRowIndices.length > 0 && (
        <HeatmapHoverLayer
          layerIndex={layerIndex}
          env={env}
          hoveredRowIndices={hoveredRowIndices}
        />
      )}
    </>
  )
}
