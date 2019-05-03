// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Grid} from '@influxdata/clockface'
import Geom from 'src/timeMachine/components/view_options/Geom'
import YAxisTitle from 'src/timeMachine/components/view_options/YAxisTitle'
import YAxisAffixes from 'src/timeMachine/components/view_options/YAxisAffixes'
import ColorSelector from 'src/timeMachine/components/view_options/ColorSelector'
import AutoDomainInput from 'src/shared/components/AutoDomainInput'

// Actions
import {
  setStaticLegend,
  setColors,
  setYAxisLabel,
  setYAxisPrefix,
  setYAxisSuffix,
  setYAxisBase,
  setYAxisScale,
  setYAxisBounds,
  setGeom,
} from 'src/timeMachine/actions'

// Utils
import {parseBounds} from 'src/shared/utils/vis'

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
  onUpdateYAxisPrefix: (prefix: string) => void
  onUpdateYAxisSuffix: (suffix: string) => void
  onUpdateYAxisBase: (base: string) => void
  onUpdateYAxisScale: (scale: string) => void
  onUpdateYAxisBounds: (bounds: Axes['y']['bounds']) => void
  onToggleStaticLegend: (isStaticLegend: boolean) => void
  onUpdateColors: (colors: Color[]) => void
  onSetGeom: (geom: XYViewGeom) => void
}

type Props = OwnProps & DispatchProps

class LineOptions extends PureComponent<Props> {
  public render() {
    const {
      axes: {
        y: {label, prefix, suffix},
      },
      colors,
      geom,
      onUpdateColors,
      onUpdateYAxisLabel,
      onUpdateYAxisPrefix,
      onUpdateYAxisSuffix,
      onSetGeom,
    } = this.props

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
        <AutoDomainInput
          domain={this.yDomain}
          onSetDomain={this.handleSetYDomain}
          label="Set Y Axis Domain"
        />
        <YAxisAffixes
          prefix={prefix}
          suffix={suffix}
          onUpdateYAxisPrefix={onUpdateYAxisPrefix}
          onUpdateYAxisSuffix={onUpdateYAxisSuffix}
        />
      </>
    )
  }

  private get yDomain(): [number, number] {
    return parseBounds(this.props.axes.y.bounds)
  }

  private handleSetYDomain = (yDomain: [number, number]): void => {
    let bounds: [string, string] | [null, null]

    if (yDomain) {
      bounds = [String(yDomain[0]), String(yDomain[1])]
    } else {
      bounds = [null, null]
    }

    this.props.onUpdateYAxisBounds(bounds)
  }
}

const mdtp: DispatchProps = {
  onUpdateYAxisLabel: setYAxisLabel,
  onUpdateYAxisPrefix: setYAxisPrefix,
  onUpdateYAxisSuffix: setYAxisSuffix,
  onUpdateYAxisBase: setYAxisBase,
  onUpdateYAxisScale: setYAxisScale,
  onUpdateYAxisBounds: setYAxisBounds,
  onToggleStaticLegend: setStaticLegend,
  onUpdateColors: setColors,
  onSetGeom: setGeom,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(LineOptions)
