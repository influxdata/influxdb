import React, {
  useReducer,
  useEffect,
  useRef,
  useMemo,
  SFC,
  CSSProperties,
} from 'react'

import {Table, PlotEnv, CATEGORY_10} from 'src/minard'
import {Axes} from 'src/minard/components/Axes'
import {plotEnvReducer, INITIAL_PLOT_ENV} from 'src/minard/utils/plotEnvReducer'
import {useMousePos} from 'src/minard/utils/useMousePos'
import {
  setDimensions,
  setTable,
  setColors,
} from 'src/minard/utils/plotEnvActions'

export interface Props {
  // Required props
  // --------------
  //
  table: Table
  width: number
  height: number
  children: (env: PlotEnv) => JSX.Element

  // Aesthetic mappings
  // ------------------
  //
  x?: string
  fill?: string[]
  // y?: string
  // start?: string
  // stop?: string
  // lower?: string
  // upper?: string
  // stroke?: string
  // strokeWidth?: string
  // shape?: ShapeKind
  // radius?: number
  // alpha?: number

  // Misc options
  // ------------
  //
  axesStroke?: string
  tickFont?: string
  tickFill?: string
  colors?: string[]
  // xBrushable?: boolean
  // yBrushable?: boolean
  // xAxisTitle?: string
  // yAxisTitle?: string
  // xAxisPrefix?: string
  // yAxisPrefix?: string
  // xAxisSuffix?: string
  // yAxisSuffix?: string
  // xTicksStroke?: string
  // yTicksStroke?: string
}

export const Plot: SFC<Props> = ({
  width,
  height,
  table,
  x,
  fill,
  children,
  colors = CATEGORY_10,
  axesStroke = '#31313d',
  tickFont = 'bold 10px Roboto',
  tickFill = '#8e91a1',
}) => {
  const [env, dispatch] = useReducer(plotEnvReducer, {
    ...INITIAL_PLOT_ENV,
    width,
    height,
    defaults: {table, colors, aesthetics: {x, fill}, scales: {}},
  })

  // TODO: Batch these on first render
  // TODO: Handle aesthetic prop changes
  useEffect(() => dispatch(setTable(table)), [table])
  useEffect(() => dispatch(setDimensions(width, height)), [width, height])
  useEffect(() => dispatch(setColors(colors)), [colors])

  const mouseRegion = useRef<HTMLDivElement>(null)
  const {x: hoverX, y: hoverY} = useMousePos(mouseRegion.current)

  const childProps = useMemo(
    () => ({
      ...env,
      hoverX,
      hoverY,
      dispatch,
    }),
    [env, hoverX, hoverY, dispatch]
  )

  const plotStyle: CSSProperties = {
    position: 'relative',
    width: `${width}px`,
    height: `${height}px`,
  }

  const layersStyle: CSSProperties = {
    position: 'absolute',
    top: `${env.margins.top}px`,
    right: `${env.margins.right}px`,
    bottom: `${env.margins.bottom}px`,
    left: `${env.margins.left}px`,
  }

  return (
    <div className="minard-plot" style={plotStyle}>
      <Axes
        env={env}
        axesStroke={axesStroke}
        tickFont={tickFont}
        tickFill={tickFill}
      >
        <div className="minard-layers" style={layersStyle}>
          {children(childProps)}
        </div>
        <div
          className="minard-interaction-region"
          style={layersStyle}
          ref={mouseRegion}
        />
      </Axes>
    </div>
  )
}
