import React, {useReducer, useRef, useMemo, SFC, CSSProperties} from 'react'

import {Table, PlotEnv} from 'src/minard'
import {Axes} from 'src/minard/components/Axes'
import {useMousePos} from 'src/minard/utils/useMousePos'
import {useMountedEffect} from 'src/minard/utils/useMountedEffect'
import {
  setDimensions,
  setTable,
  setControlledXDomain,
  setControlledYDomain,
  setXAxisLabel,
  setYAxisLabel,
} from 'src/minard/utils/plotEnvActions'
import {plotEnvReducer, INITIAL_PLOT_ENV} from 'src/minard/utils/plotEnvReducer'

export interface Props {
  //
  // Required props
  // ==============
  //
  table: Table
  width: number
  height: number
  children: (env: PlotEnv) => JSX.Element

  //
  // Miscellaneous options
  // =====================
  //
  axesStroke?: string
  tickFont?: string
  tickFill?: string
  xAxisLabel?: string
  yAxisLabel?: string

  // The x domain of the plot can be explicitly set. If this prop is passed,
  // then the component is operating in a "controlled" mode, where it always
  // uses the passed x domain. Any interaction with the plot that should change
  // the x domain (clicking, brushing, etc.) will call the `onSetXDomain` prop
  // when the component is in controlled mode. If the `xDomain` prop is not
  // passed, then the component is "uncontrolled". It will compute and set the
  // `xDomain` automatically.
  xDomain?: [number, number]
  onSetXDomain?: (xDomain: [number, number]) => void

  // See the `xDomain` and `onSetXDomain` props
  yDomain?: [number, number]
  onSetYDomain?: (yDomain: [number, number]) => void
}

export const SizedPlot: SFC<Props> = ({
  width,
  height,
  table,
  children,
  axesStroke = '#31313d',
  tickFont = 'bold 10px Roboto',
  tickFill = '#8e91a1',
  xAxisLabel = '',
  yAxisLabel = '',
  xDomain = null,
  yDomain = null,
}) => {
  const [env, dispatch] = useReducer(plotEnvReducer, {
    ...INITIAL_PLOT_ENV,
    width,
    height,
    xDomain,
    yDomain,
    xAxisLabel,
    yAxisLabel,
    baseLayer: {...INITIAL_PLOT_ENV.baseLayer, table},
  })

  useMountedEffect(() => dispatch(setTable(table)), [table])
  useMountedEffect(() => dispatch(setControlledXDomain(xDomain)), [xDomain])
  useMountedEffect(() => dispatch(setControlledYDomain(yDomain)), [yDomain])
  useMountedEffect(() => dispatch(setXAxisLabel(xAxisLabel)), [xAxisLabel])
  useMountedEffect(() => dispatch(setYAxisLabel(yAxisLabel)), [yAxisLabel])
  useMountedEffect(() => dispatch(setDimensions(width, height)), [
    width,
    height,
  ])

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
