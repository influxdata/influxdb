import * as React from 'react'
import {useRef, useLayoutEffect, useMemo, FunctionComponent} from 'react'

import {LineLayerConfig} from '../types'
import {PlotEnv} from '../utils/PlotEnv'
import {drawLines} from '../utils/drawLines'
import {collectLineData, simplifyLineData} from '../utils/lineData'
import {clearCanvas} from '../utils/clearCanvas'
import {LineHoverLayer} from './LineHoverLayer'
import {FILL} from '../constants/columnKeys'
import {useHoverLineIndices} from '../utils/useHoverLineIndices'

interface Props {
  env: PlotEnv
  layerIndex: number
  hoverX: number
  hoverY: number
}

export const LineLayer: FunctionComponent<Props> = ({
  env,
  layerIndex,
  hoverX,
  hoverY,
}) => {
  const table = env.getTable(layerIndex)
  const fillScale = env.getScale(layerIndex, 'fill')
  const {xScale, yScale, innerWidth: width, innerHeight: height} = env
  const layer = env.config.layers[layerIndex] as LineLayerConfig

  const {
    interpolation,
    x: xColKey,
    y: yColKey,
    lineWidth,
    hoverDimension,
    maxTooltipRows,
  } = layer

  const xColData = table.getColumn(xColKey, 'number')
  const yColData = table.getColumn(yColKey, 'number')
  const groupColData = table.getColumn(FILL, 'string')

  const lineData = useMemo(
    () => collectLineData(table, xColKey, yColKey, fillScale),
    [table, xColKey, yColKey, fillScale]
  )

  // TODO: Simplify in data domain, resimplify when dimensions change on a
  // debounced timer (for fast resizes)
  const simplifiedLineData = useMemo(
    () => simplifyLineData(lineData, xScale, yScale),
    [lineData, xScale, yScale]
  )

  const resolvedHoverDimension =
    hoverDimension === 'auto'
      ? Object.keys(lineData).length > maxTooltipRows
        ? 'xy'
        : 'x'
      : hoverDimension

  const canvasRef = useRef<HTMLCanvasElement>(null)

  useLayoutEffect(() => {
    clearCanvas(canvasRef.current, width, height)
    drawLines({
      canvas: canvasRef.current,
      lineData: simplifiedLineData,
      interpolation,
      lineWidth,
    })
  }, [simplifiedLineData, canvasRef.current, interpolation, width, height])

  const hoverRowIndices = useHoverLineIndices(
    resolvedHoverDimension,
    hoverX,
    hoverY,
    xColData,
    yColData,
    groupColData,
    xScale,
    yScale,
    width,
    height
  )

  const hasHoverData = hoverRowIndices && hoverRowIndices.length > 0

  return (
    <>
      <canvas
        className="vis-layer line"
        ref={canvasRef}
        style={{
          position: 'absolute',
          opacity: resolvedHoverDimension === 'xy' && hasHoverData ? 0.4 : 1,
        }}
      />
      {hasHoverData && (
        <LineHoverLayer
          env={env}
          layerIndex={layerIndex}
          lineData={simplifiedLineData}
          rowIndices={hoverRowIndices}
          dimension={resolvedHoverDimension}
        />
      )}
    </>
  )
}
