// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Grid} from '@influxdata/clockface'
import Geom from 'src/timeMachine/components/view_options/Geom'
import YAxisTitle from 'src/timeMachine/components/view_options/YAxisTitle'
import AxisAffixes from 'src/timeMachine/components/view_options/AxisAffixes'
import ColorSelector from 'src/timeMachine/components/view_options/ColorSelector'
import AutoDomainInput from 'src/shared/components/AutoDomainInput'

// Actions
import {
  setColors,
  setYAxisLabel,
  setAxisPrefix,
  setAxisSuffix,
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
  onUpdateYAxisLabel: typeof setYAxisLabel
  onUpdateAxisPrefix: typeof setAxisPrefix
  onUpdateAxisSuffix: typeof setAxisSuffix
  onUpdateYAxisBounds: typeof setYAxisBounds
  onUpdateColors: typeof setColors
  onSetGeom: typeof setGeom
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
      onUpdateAxisPrefix,
      onUpdateAxisSuffix,
      onSetGeom,
    } = this.props

    return (
      <>
        <Grid.Column>
          <h4 className="view-options--header">Customize Line Graph</h4>
          <h5 className="view-options--header">Options</h5>
        </Grid.Column>
        {geom && <Geom geom={geom} onSetGeom={onSetGeom} />}
        <ColorSelector
          colors={colors.filter(c => c.type === 'scale')}
          onUpdateColors={onUpdateColors}
        />
        <Grid.Column>
          <h5 className="view-options--header">Y Axis</h5>
        </Grid.Column>
        <YAxisTitle label={label} onUpdateYAxisLabel={onUpdateYAxisLabel} />
        <AxisAffixes
          prefix={prefix}
          suffix={suffix}
          axisName="y"
          onUpdateAxisPrefix={prefix => onUpdateAxisPrefix(prefix, 'y')}
          onUpdateAxisSuffix={suffix => onUpdateAxisSuffix(suffix, 'y')}
        />
        <Grid.Column>
          <AutoDomainInput
            domain={this.yDomain}
            onSetDomain={this.handleSetYDomain}
            label="Y Axis Domain"
          />
        </Grid.Column>
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
  onUpdateAxisPrefix: setAxisPrefix,
  onUpdateAxisSuffix: setAxisSuffix,
  onUpdateYAxisBounds: setYAxisBounds,
  onUpdateColors: setColors,
  onSetGeom: setGeom,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(LineOptions)
