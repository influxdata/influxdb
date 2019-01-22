// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Grid} from 'src/clockface'
import Geom from 'src/shared/components/view_options/options/Geom'
import YAxisTitle from 'src/shared/components/view_options/options/YAxisTitle'
import YAxisBounds from 'src/shared/components/view_options/options/YAxisBounds'
import YAxisAffixes from 'src/shared/components/view_options/options/YAxisAffixes'
import YAxisBase from 'src/shared/components/view_options/options/YAxisBase'
import YAxisScale from 'src/shared/components/view_options/options/YAxisScale'
import ColorSelector from 'src/shared/components/view_options/options/ColorSelector'

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
} from 'src/shared/actions/v2/timeMachines'

// Types
import {ViewType} from 'src/types/v2'
import {Axes, XYViewGeom} from 'src/types/v2/dashboards'
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
