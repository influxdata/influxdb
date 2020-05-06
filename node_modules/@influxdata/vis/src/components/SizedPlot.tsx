import * as React from 'react'
import {
  useRef,
  useMemo,
  useCallback,
  FunctionComponent,
  CSSProperties,
} from 'react'

import {Axes} from './Axes'
import {SizedConfig} from '../types'
import {HistogramLayer} from './HistogramLayer'
import {LineLayer} from './LineLayer'
import {HeatmapLayer} from './HeatmapLayer'
import {ScatterLayer} from './ScatterLayer'
import {Brush} from './Brush'
import {rangeToDomain} from '../utils/brush'
import {usePlotEnv} from '../utils/usePlotEnv'
import {useMousePos} from '../utils/useMousePos'
import {useDragEvent} from '../utils/useDragEvent'
import {useForceUpdate} from '../utils/useForceUpdate'

interface Props {
  config: SizedConfig
}

export const SizedPlot: FunctionComponent<Props> = ({config, children}) => {
  const env = usePlotEnv(config)

  const {
    margins,
    config: {width, height, showAxes},
  } = env

  const plotStyle = useMemo(
    () =>
      ({
        position: 'relative',
        width: `${width}px`,
        height: `${height}px`,
        userSelect: 'none',
      } as CSSProperties),
    [width, height]
  )

  const innerPlotStyle = useMemo(
    () =>
      ({
        position: 'absolute',
        top: `${margins.top}px`,
        right: `${margins.right}px`,
        bottom: `${margins.bottom}px`,
        left: `${margins.left}px`,
        cursor: 'crosshair',
      } as CSSProperties),
    [margins]
  )

  const fullsizeStyle: CSSProperties = {
    position: 'absolute',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
  }

  const forceUpdate = useForceUpdate()
  const innerPlotRef = useRef<HTMLDivElement>(null)
  const [hoverEvent, hoverProps] = useMousePos()
  const dragEvent = useDragEvent(innerPlotRef.current)
  const hoverX = dragEvent ? null : hoverEvent.x
  const hoverY = dragEvent ? null : hoverEvent.y

  const handleXBrushEnd = useCallback(
    (xRange: number[]) => {
      env.xDomain = rangeToDomain(xRange, env.xScale, env.innerWidth)
      forceUpdate()
    },
    [env.xScale, env.innerWidth, forceUpdate]
  )

  const handleYBrushEnd = useCallback(
    (yRange: number[]) => {
      env.yDomain = rangeToDomain(yRange, env.yScale, env.innerHeight).reverse()
      forceUpdate()
    },
    [env.yScale, env.innerHeight, forceUpdate]
  )

  const handleResetDomains = useCallback(() => {
    env.resetDomains()
    forceUpdate()
  }, [env])

  return (
    <div className="vis-plot" style={plotStyle}>
      {showAxes && <Axes env={env} style={fullsizeStyle} />}
      <div
        className="vis-inner-plot"
        style={innerPlotStyle}
        ref={innerPlotRef}
        onDoubleClick={handleResetDomains}
        {...hoverProps}
      >
        <div className="vis-layers" style={fullsizeStyle}>
          {config.layers.map((layer, i) => {
            switch (layer.type) {
              case 'line':
                return (
                  <LineLayer
                    key={i}
                    layerIndex={i}
                    env={env}
                    hoverX={hoverX}
                    hoverY={hoverY}
                  />
                )
              case 'histogram':
                return (
                  <HistogramLayer
                    key={i}
                    layerIndex={i}
                    env={env}
                    hoverX={hoverX}
                    hoverY={hoverY}
                  />
                )
              case 'heatmap':
                return (
                  <HeatmapLayer
                    key={i}
                    layerIndex={i}
                    env={env}
                    hoverX={hoverX}
                    hoverY={hoverY}
                  />
                )
              case 'scatter':
                return <ScatterLayer key={i} layerIndex={i} env={env} />
              default:
                return null
            }
          })}
          {children && children}
        </div>
        <Brush
          event={dragEvent}
          width={env.innerWidth}
          height={env.innerHeight}
          onXBrushEnd={handleXBrushEnd}
          onYBrushEnd={handleYBrushEnd}
        />
      </div>
    </div>
  )
}
