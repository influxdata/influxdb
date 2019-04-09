// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Grid} from '@influxdata/clockface'
import Geom from 'src/timeMachine/components/view_options/Geom'
import YAxisTitle from 'src/timeMachine/components/view_options/YAxisTitle'
import YAxisBounds from 'src/timeMachine/components/view_options/YAxisBounds'
import YAxisAffixes from 'src/timeMachine/components/view_options/YAxisAffixes'
import YAxisBase from 'src/timeMachine/components/view_options/YAxisBase'
import YAxisScale from 'src/timeMachine/components/view_options/YAxisScale'
import ColorSelector from 'src/timeMachine/components/view_options/ColorSelector'

// Actions
import {
  setStaticLegend,
  setColors,
  setYAxisLabel,
  setYAxisMinBound,
  setYAxisMaxBound,
  setYAxisPrefix,
  setYAxisSuffix,
  setYAxisBase,
  setYAxisScale,
  setGeom,
} from 'src/timeMachine/actions'

// Types
import {ViewType} from 'src/types'
import {Axes, XYViewGeom} from 'src/types/dashboards'
import {Color} from 'src/types/colors'

interface OwnProps {
  type: ViewType
  axes: Axes
  geom?: XYViewGeom
  colors: Color[]
}

interface DispatchProps {
  onUpdateYAxisLabel: (label: string) => void
  onUpdateYAxisMinBound: (min: string) => void
  onUpdateYAxisMaxBound: (max: string) => void
  onUpdateYAxisPrefix: (prefix: string) => void
  onUpdateYAxisSuffix: (suffix: string) => void
  onUpdateYAxisBase: (base: string) => void
  onUpdateYAxisScale: (scale: string) => void
  onToggleStaticLegend: (isStaticLegend: boolean) => void
  onUpdateColors: (colors: Color[]) => void
  onSetGeom: (geom: XYViewGeom) => void
}

type Props = OwnProps & DispatchProps

class LineOptions extends PureComponent<Props> {
  public render() {
    const {
      axes: {
        y: {label, bounds, scale, prefix, suffix, base},
      },
      colors,
      geom,
      onUpdateColors,
      onUpdateYAxisLabel,
      onUpdateYAxisMinBound,
      onUpdateYAxisMaxBound,
      onUpdateYAxisPrefix,
      onUpdateYAxisSuffix,
      onUpdateYAxisBase,
      onUpdateYAxisScale,
      onSetGeom,
    } = this.props

    const [min, max] = bounds

    return (
      <>
        <Grid.Column>
          <h4 className="view-options--header">Customize Graph</h4>
        </Grid.Column>
        {geom && <Geom geom={geom} onSetGeom={onSetGeom} />}
        <ColorSelector
          colors={colors.filter(c => c.type === 'scale')}
          onUpdateColors={onUpdateColors}
        />
        <Grid.Column>
          <h4 className="view-options--header">Left Y Axis</h4>
        </Grid.Column>
        <YAxisTitle label={label} onUpdateYAxisLabel={onUpdateYAxisLabel} />
        <YAxisBounds
          min={min}
          max={max}
          scale={scale}
          onUpdateYAxisMaxBound={onUpdateYAxisMaxBound}
          onUpdateYAxisMinBound={onUpdateYAxisMinBound}
        />
        <YAxisAffixes
          prefix={prefix}
          suffix={suffix}
          onUpdateYAxisPrefix={onUpdateYAxisPrefix}
          onUpdateYAxisSuffix={onUpdateYAxisSuffix}
        />
        <YAxisBase base={base} onUpdateYAxisBase={onUpdateYAxisBase} />
        <YAxisScale scale={scale} onUpdateYAxisScale={onUpdateYAxisScale} />
      </>
    )
  }
}

const mdtp: DispatchProps = {
  onUpdateYAxisLabel: setYAxisLabel,
  onUpdateYAxisMinBound: setYAxisMinBound,
  onUpdateYAxisMaxBound: setYAxisMaxBound,
  onUpdateYAxisPrefix: setYAxisPrefix,
  onUpdateYAxisSuffix: setYAxisSuffix,
  onUpdateYAxisBase: setYAxisBase,
  onUpdateYAxisScale: setYAxisScale,
  onToggleStaticLegend: setStaticLegend,
  onUpdateColors: setColors,
  onSetGeom: setGeom,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(LineOptions)
